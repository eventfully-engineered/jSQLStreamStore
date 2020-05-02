package com.eventfullyengineered.jsqlstreamstore.sqlite.sqlite;

import com.eventfullyengineered.jsqlstreamstore.StreamNotFoundException;
import com.eventfullyengineered.jsqlstreamstore.common.ResultSets;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.Empty;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import com.eventfullyengineered.jsqlstreamstore.streams.*;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.AllStreamSubscription;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.StreamSubscription;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.SubscriptionDropped;
import com.eventfullyengineered.jsqlstreamstore.store.ConnectionFactory;
import com.eventfullyengineered.jsqlstreamstore.store.StreamStoreBase;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.AllStreamMessageReceived;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.AllSubscriptionDropped;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.HasCaughtUp;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.StreamMessageReceived;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.sqlite.SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE;

// TODO: set PreparedStatement fetchSize?
// TODO: catch stream name to stream id to avoid query if needed
// TODO: change to list instead of arrays

public class SqliteStreamStore extends StreamStoreBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqliteStreamStore.class);

    private final ConnectionFactory connectionFactory;
    private final Scripts scripts;
//    private final Supplier<StreamStoreNotifier> streamStoreNotifier;
    private final JsonSerializerStrategy jsonSerializerStrategy;

    public SqliteStreamStore(SqliteStreamStoreSettings settings) {
        super(Duration.ofMinutes(1), 10000);
//        super(settings.getMetadataMaxAgeCacheExpire(), settings.getMetadataMaxAgeCacheMaxSize());

        connectionFactory = settings.getConnectionFactory();
        jsonSerializerStrategy = settings.getJsonSerializerStrategy();

//        streamStoreNotifier = () -> {
//            if (settings.getCreateStreamStoreNotifier() == null) {
//                throw new RuntimeException("Cannot create notifier because supplied createStreamStoreNotifier was null");
//            }
//            return settings.getCreateStreamStoreNotifier().createStreamStoreNotifier(this);
//        };

        scripts = new Scripts();
    }

    @Override
    protected long getStreamMessageCount(String streamName) throws SQLException {
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(scripts.getStreamMessageCount())) {
            stmt.setString(1, streamName);
            try (ResultSet result = stmt.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    @Override
    protected AppendResult appendToStreamInternal(String streamName, long expectedVersion, NewStreamMessage[] messages) throws SQLException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName));
        Preconditions.checkArgument(expectedVersion >= -2);
        Preconditions.checkArgument(messages != null && messages.length > 0, "messages must not be null or empty");

        try (Connection connection = connectionFactory.openConnection()) {
            // TODO: strange to get AppendResult here and then return a new AppendResult
            // SqlStreamStore returns MsSqlAppendResult/PostgresAppendResult/etc here then converts it to common AppendResult
            AppendResult result = appendToStreamInternal(connection, streamName, expectedVersion, messages);

            if (result.getMaxCount() != null) {
                checkStreamMaxCount(streamName, result.getMaxCount());
            }

            return new AppendResult(result.getCurrentVersion(), result.getCurrentPosition());
        }
    }

    // TODO: idempotent writes. EventStore has a good break down of cases
    // https://eventstore.com/docs/dotnet-api/optimistic-concurrency-and-idempotence/index.html
    // if we didnt have a stream table what would it look like?
    // TODO: try this in branch
    // get latest version and if null it would be -1 (no stream)
    // write batch
    // if no stream append create metadata stream (append to metadata)
    // otherwise get last metadata message
    private AppendResult appendToStreamInternal(Connection connection,
                                                String streamName,
                                                long expectedVersion,
                                                NewStreamMessage[] messages) throws SQLException {
        // TODO: wrap in logic to retry if deadlock
        StreamDetails streamDetails = null;
        try {
            connection.setAutoCommit(false);

            streamDetails = getOrCreateStreamDetail(connection, streamName);
            // TODO: verify this is correct for all versions and check works here
            // should follow something like what eventstore does
            // https://eventstore.com/docs/dotnet-api/optimistic-concurrency-and-idempotence/index.html
            if (expectedVersion > streamDetails.getVersion()) {
                throw new WrongExpectedVersion(streamName, streamDetails.getVersion(), expectedVersion);
            }

            batchInsert(connection, streamDetails.getId(), messages, streamDetails.getVersion());

            StreamMessage lastStreamMessage = getLastStreamMessage(connection, streamName);

            // should never be null here
            assert lastStreamMessage != null;
            
            updateStream(connection, streamDetails.getId(), lastStreamMessage.getStreamVersion(), lastStreamMessage.getPosition());

            connection.commit();

            return new AppendResult(streamDetails.getMaxCount(), lastStreamMessage.getStreamVersion(), lastStreamMessage.getPosition());
        } catch (SQLException ex) {
            connection.rollback();
            if (ex instanceof SQLiteException) {
                SQLiteException sqliteException = (SQLiteException) ex;
                if (SQLITE_CONSTRAINT_UNIQUE == sqliteException.getResultCode()) {

                    long streamVersion = streamDetails != null
                        ? streamDetails.getVersion()
                        : -1;

                    ReadStreamPage page = readStreamInternal(
                        streamName,
                        StreamVersion.START,
                        messages.length,
                        ReadDirection.FORWARD,
                        false,
                        null,
                        connection);

                    if (messages.length > page.getMessages().length) {
                        throw new WrongExpectedVersion(streamName, streamVersion, expectedVersion, ex);
                    }

                    for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                        if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                            throw new WrongExpectedVersion(streamName, streamVersion, expectedVersion, ex);
                        }
                    }

                    // TODO: get max count
                    return new AppendResult(
                        null,
                        page.getLastStreamVersion(),
                        page.getLastStreamPosition());
                }
            }

            throw ex;
        }

    }

    private void batchInsert(Connection connection,
                             Integer streamId,
                             NewStreamMessage[] messages,
                             long latestStreamVersion) throws SQLException {
        // TODO: configurable batch size?
        final int batchSize = 1000;
        // TODO: just use the table default
        LocalDateTime date = LocalDateTime.now(ZoneId.of("UTC"));
        try (PreparedStatement ps = connection.prepareStatement(scripts.writeMessage())) {
            int count = 0;
            for (int i = 0; i < messages.length; i++) {
                NewStreamMessage message = messages[i];
                ps.setObject(1, message.getMessageId());
                ps.setInt(2, streamId);
                ps.setLong(3, latestStreamVersion + i + 1);
                ps.setString(4, DateTimeFormatter.ISO_DATE_TIME.format(date));
                ps.setString(5, message.getType());
                ps.setString(6, message.getData());
                ps.setString(7, message.getMetadata());
                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            // TODO: dont need to call if equal to mod batch size
            ps.executeBatch();
        }
    }

    @Override
    protected void deleteStreamInternal(String streamName, long expectedVersion) throws SQLException {
        if (expectedVersion == ExpectedVersion.ANY) {
            deleteStreamAnyVersion(streamName);
        } else {
            deleteStreamExpectedVersion(streamName, expectedVersion);
        }
    }

    private void deleteStreamExpectedVersion(String streamName, long expectedVersion) throws SQLException {
        try (Connection connection = connectionFactory.openConnection();
             CallableStatement stmt = connection.prepareCall(scripts.deleteStreamExpectedVersion())) {

            connection.setAutoCommit(false);
            stmt.setString(1, streamName);
            stmt.setLong(2, expectedVersion);
            stmt.executeUpdate();

            NewStreamMessage streamDeletedEvent = Deleted.createStreamDeletedMessage(streamName);
            appendToStreamInternal(connection, Deleted.DELETED_STREAM_ID, ExpectedVersion.ANY, new NewStreamMessage[] { streamDeletedEvent });

            // Delete metadata stream (if it exists)
            deleteStreamAnyVersion(connection, streamName);

            connection.commit();
        }
    }

    private void deleteStreamAnyVersion(String streamName) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);
            deleteStreamAnyVersion(connection, streamName);
            connection.commit();
        }
    }

    private void deleteStreamAnyVersion(Connection connection, String streamName) throws SQLException {
        boolean aStreamIsDeleted;
        try (PreparedStatement deleteStreamStmt = connection.prepareStatement(scripts.deleteStream());
             PreparedStatement deleteStreamMessages = connection.prepareStatement(scripts.deleteStreamMessages())) {

            deleteStreamStmt.setString(1, streamName);
            int count = deleteStreamStmt.executeUpdate();
            aStreamIsDeleted = count > 0;

            deleteStreamMessages.setString(1, streamName);
            int deleteCount = deleteStreamMessages.executeUpdate();
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        }

        if (aStreamIsDeleted) {
            NewStreamMessage streamDeletedEvent = Deleted.createStreamDeletedMessage(streamName);
            appendToStreamInternal(connection, Deleted.DELETED_STREAM_ID, ExpectedVersion.ANY, new NewStreamMessage[] { streamDeletedEvent });
        }

    }

    @Override
    protected void deleteMessageInternal(String streamName, UUID messageId) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);

            StreamDetails details = getStreamDetails(connection, streamName);

            try (PreparedStatement stmt = connection.prepareStatement(scripts.deleteStreamMessage())) {
                stmt.setInt(1, details.getId());
                stmt.setObject(2, messageId);

                stmt.execute();
                if (stmt.getUpdateCount() == 1) {
                    NewStreamMessage deletedMessage = Deleted.createMessageDeletedMessage(streamName, messageId);
                    appendToStreamInternal(connection, Deleted.DELETED_STREAM_ID, ExpectedVersion.ANY, new NewStreamMessage[] { deletedMessage });
                }
            }
            connection.commit();
        }
    }

    @Override
    protected SetStreamMetadataResult setStreamMetadataInternal(String streamName, long expectedStreamMetadataVersion, Integer maxAge, Long maxCount, String metadataJson) throws SQLException {
        return null;
    }

    @Override
    protected StreamSubscription subscribeToStreamInternal(String streamId, Integer startVersion, StreamMessageReceived streamMessageReceived, SubscriptionDropped subscriptionDropped, HasCaughtUp hasCaughtUp, boolean prefetchJsonData, String name) {
        return null;
    }

    @Override
    protected AllStreamSubscription subscribeToAllInternal(Long fromPosition, AllStreamMessageReceived streamMessageReceived, AllSubscriptionDropped subscriptionDropped, HasCaughtUp hasCaughtUp, boolean prefetchJsonData, String name) {
        return null;
    }

    @Override
    protected ReadAllPage readAllForwardsInternal(long fromPositionExclusive,
                                                  int maxCount,
                                                  boolean prefetch,
                                                  ReadNextAllPage readNextAllPage) throws SQLException {
        long ordinal = fromPositionExclusive;

        int resultSetCount = 0;
        String commandText = prefetch ? scripts.readAllForwardWithData() : scripts.readAllForward();
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(commandText)) {

            stmt.setLong(1, ordinal);
            // Read extra row to see if at end or not
            stmt.setLong(2, maxCount == Long.MAX_VALUE ? maxCount : maxCount + 1);

            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    resultSetCount++;
                    if (messages.size() < maxCount) {
                        int streamId = result.getInt(1);
                        String streamName = result.getString(2);
                        int streamVersion = result.getInt(3);
                        ordinal = result.getLong(4);
                        UUID messageId = UUID.fromString(result.getString(5));
                        // TODO: I dont think we need or want this
                        LocalDateTime created = LocalDateTime.parse(result.getString(6));
                        String type = result.getString(7);
                        String metadata = result.getString(8);

                        // TODO: improve
                        final StreamMessage message;
                        if (prefetch) {
                            message = new StreamMessage(
                                streamName,
                                messageId,
                                streamVersion,
                                ordinal,
                                created,
                                type,
                                metadata,
                                result.getString(9)
                            );
                        } else {
                            message = new StreamMessage(
                                streamName,
                                messageId,
                                streamVersion,
                                ordinal,
                                created,
                                type,
                                metadata,
                                () -> getJsonData(streamId, streamVersion)
                            );
                        }
                        messages.add(message);
                    }
                }
            }

            if (messages.isEmpty()) {
                return new ReadAllPage(
                    fromPositionExclusive,
                    fromPositionExclusive,
                    true,
                    ReadDirection.FORWARD,
                    readNextAllPage,
                    Empty.STREAM_MESSAGE);
            }

            long nextPosition = Iterables.getLast(messages).getPosition() + 1;
            return new ReadAllPage(
                fromPositionExclusive,
                nextPosition,
                maxCount >= resultSetCount,
                ReadDirection.FORWARD,
                readNextAllPage,
                messages.toArray(new StreamMessage[0]));
        }
    }

    @Override
    protected ReadAllPage readAllBackwardsInternal(long fromPositionExclusive,
                                                   int maxCount,
                                                   boolean prefetch,
                                                   ReadNextAllPage readNextAllPage) throws SQLException {
        // TODO: fix...should be int because of list remove
        long ordinal = fromPositionExclusive == Position.END ? Long.MAX_VALUE : fromPositionExclusive;

        int resultSetCount = 0;
        String commandText = prefetch ? scripts.readAllBackwardWithData() : scripts.readAllBackward();
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(commandText)) {

            stmt.setLong(1, ordinal);
            // Read extra row to see if at end or not
            stmt.setLong(2, maxCount == Long.MAX_VALUE ? maxCount : maxCount + 1);

            long lastOrdinal = 0;
            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    resultSetCount++;
                    int streamId = result.getInt(1);
                    String streamName = result.getString(2);
                    int streamVersion = result.getInt(3);
                    ordinal = result.getLong(4);
                    lastOrdinal = ordinal;
                    UUID messageId = UUID.fromString(result.getString(5));
                    // TODO: I dont think we need or want this
                    LocalDateTime created = LocalDateTime.parse(result.getString(6));
                    String type = result.getString(7);
                    String metadata = result.getString(8);

                    // TODO: improve
                    final StreamMessage message;
                    if (prefetch) {
                        message = new StreamMessage(
                            streamName,
                            messageId,
                            streamVersion,
                            ordinal,
                            created,
                            type,
                            metadata,
                            result.getString(9)
                        );
                    } else {
                        message = new StreamMessage(
                            streamName,
                            messageId,
                            streamVersion,
                            ordinal,
                            created,
                            type,
                            metadata,
                            () -> getJsonData(streamId, streamVersion)
                        );
                    }

                    messages.add(message);
                }
            }

            if (messages.isEmpty()) {
                return new ReadAllPage(
                    fromPositionExclusive,
                    fromPositionExclusive,
                    true,
                    ReadDirection.BACKWARD,
                    null,
                    Empty.STREAM_MESSAGE);
            }

            // An extra row was read, we're not at the end
            boolean isEnd = resultSetCount != maxCount + 1;
            if (!isEnd) {
                messages.remove(maxCount);
            }

            long nextPosition = messages.get(0).getPosition();
            return new ReadAllPage(
                nextPosition,
                lastOrdinal,
                isEnd,
                ReadDirection.BACKWARD,
                readNextAllPage,
                messages.toArray(new StreamMessage[0]));
        }
    }

    @Override
    protected ReadStreamPage readStreamForwardsInternal(String streamName,
                                                        long start,
                                                        int count,
                                                        boolean prefetch,
                                                        ReadNextStreamPage readNext) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            return readStreamInternal(
                streamName,
                start,
                count,
                ReadDirection.FORWARD,
                prefetch,
                readNext,
                connection);
        }
    }

    @Override
    protected ReadStreamPage readStreamBackwardsInternal(String streamName,
                                                         long start,
                                                         int count,
                                                         boolean prefetch,
                                                         ReadNextStreamPage readNext) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            return readStreamInternal(
                streamName,
                start,
                count,
                ReadDirection.BACKWARD,
                prefetch,
                readNext,
                connection);
        }
    }

    @Override
    protected Long readHeadPositionInternal() {
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(scripts.readHeadPosition());
             ResultSet result = stmt.executeQuery()) {
            result.next();
            long head = result.getLong(1);
            if (result.wasNull()) {
                // TODO: use constant
                return -1L;
            }
            return head;
        } catch (SQLException ex) {
            // TODO: do we want to throw SqlException?
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected StreamMetadataResult getStreamMetadataInternal(String streamName) throws SQLException {
        final StreamMessage message;
        try (Connection connection = connectionFactory.openConnection()) {
            message = getLastStreamMessage(connection, streamName);
        }

        if (message == null) {
            return new StreamMetadataResult(streamName, -1);
        }

        MetadataMessage metadataMessage = jsonSerializerStrategy.fromJson(message.getJsonData(), MetadataMessage.class);
        return new StreamMetadataResult(
            streamName,
            message.getStreamVersion(),
            metadataMessage.getMaxAge(),
            metadataMessage.getMaxCount(),
            metadataMessage.getMetaJson());
    }

    // TODO: make async/reactive
    private void checkStreamMaxCount(String streamName, Long maxCount) throws SQLException {
        if (maxCount != null) {
            long count = getStreamMessageCount(streamName);
            if (count > maxCount) {
                long totalToPurge = count - maxCount;
                while (totalToPurge > 0) {

                    int pageSize = totalToPurge > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) totalToPurge;
                    ReadStreamPage streamMessagesPage = readStreamForwardsInternal(
                        streamName,
                        StreamVersion.START,
                        pageSize,
                        false,
                        null);

                    // TODO: batch?
                    if (streamMessagesPage.getStatus() == PageReadStatus.SUCCESS) {
                        for (StreamMessage message : streamMessagesPage.getMessages()) {
                            deleteMessageInternal(streamName, message.getMessageId());
                        }
                    }

                    totalToPurge = count;
                }
            }
        }
    }



    private ReadStreamPage readStreamInternal(String streamName,
                                              long start,
                                              long count,
                                              ReadDirection direction,
                                              boolean prefetch,
                                              ReadNextStreamPage readNext,
                                              Connection connection) throws SQLException {

        try {
            // To read backwards from end, need to use int MaxValue
            long startVersion = start == StreamVersion.END ? Long.MAX_VALUE : start;
            String commandText;
            GetNextVersion getNextVersion;
            if (direction == ReadDirection.FORWARD) {
                commandText = prefetch ? scripts.readStreamForwardWithData() : scripts.readStreamForward();
                getNextVersion = (List<StreamMessage> messages, long lastVersion) -> {
                    if (messages != null && !messages.isEmpty()) {
                        return Iterables.getLast(messages).getStreamVersion() + 1;
                    }
                    return lastVersion + 1;
                };
            } else {
                commandText = prefetch ? scripts.readStreamBackwardsWithData() : scripts.readStreamBackwards();
                getNextVersion = (List<StreamMessage> messages, long lastVersion) -> {
                    if (messages != null && !messages.isEmpty()) {
                        return Iterables.getLast(messages).getStreamVersion() - 1;
                    }
                    return lastVersion - 1;
                };
            }

            StreamDetails streamDetails = getStreamDetails(connection, streamName);
            if (streamDetails.isEmpty()) {
                return new ReadStreamPage(
                    streamName,
                    PageReadStatus.STREAM_NOT_FOUND,
                    start,
                    StreamVersion.END,
                    StreamVersion.END,
                    Position.END,
                    direction,
                    true,
                    readNext
                );
            }

            try (PreparedStatement stmt = connection.prepareStatement(commandText)) {
                stmt.setInt(1, streamDetails.getId());
                stmt.setLong(2, startVersion);
                stmt.setLong(3, count == Long.MAX_VALUE ? count : count + 1);

                int resultSetCount = 0;
                List<StreamMessage> messages = new ArrayList<>();
                try (ResultSet result = stmt.executeQuery()) {
                    while (result.next()) {
                        if (messages.size() < count) {
                            int streamId = result.getInt(1);
                            long version = result.getLong(2);
                            long position = result.getLong(3);
                            UUID messageId = UUID.fromString(result.getString(4));
                            // TODO: I dont think we need or want this
                            LocalDateTime created = LocalDateTime.parse(result.getString(5));
                            String type = result.getString(6);
                            String metadata = result.getString(7);

                            // TODO: improve
                            final StreamMessage message;
                            if (prefetch) {
                                message = new StreamMessage(
                                    streamName,
                                    messageId,
                                    version,
                                    position,
                                    created,
                                    type,
                                    metadata,
                                    result.getString(8)
                                );

                            } else {
                                message = new StreamMessage(
                                    streamName,
                                    messageId,
                                    version,
                                    position,
                                    created,
                                    type,
                                    metadata,
                                    () -> getJsonData(streamId, version)
                                );
                            }

                            messages.add(message);
                        }
                        resultSetCount = result.getRow();
                    }
                }
                if (messages.isEmpty()) {
                    return new ReadStreamPage(
                        streamName,
                        PageReadStatus.STREAM_NOT_FOUND,
                        start,
                        StreamVersion.END,
                        StreamVersion.END,
                        Position.END,
                        direction,
                        true,
                        readNext
                    );
                }

                return new ReadStreamPage(
                    streamName,
                    PageReadStatus.SUCCESS,
                    start,
                    getNextVersion.get(messages, streamDetails.getVersion()),
                    streamDetails.getVersion(),
                    streamDetails.getPosition(),
                    direction,
                    count >= resultSetCount,
                    readNext,
                    messages.toArray(Empty.STREAM_MESSAGE));
            }
        } catch (StreamNotFoundException ex) {
            return new ReadStreamPage(
                streamName,
                PageReadStatus.STREAM_NOT_FOUND,
                start,
                StreamVersion.END,
                StreamVersion.END,
                Position.END,
                direction,
                true,
                readNext
            );
        }
    }


    // TODO: make this reactive/async?
    private String getJsonData(int streamId, long streamVersion) throws SQLException {
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(scripts.readMessageData())) {
            stmt.setInt(1, streamId);
            stmt.setLong(2, streamVersion);

            try (ResultSet result = stmt.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }

        }
    }

    private StreamDetails getStreamDetails(Connection connection, String streamName) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(scripts.getStreamDetails())) {
            stmt.setString(1, streamName);

            try (ResultSet rs = stmt.executeQuery()) {
                // TODO: should we throw StreamNotFoundException or return an "empty" Null Object ?
                if (!rs.next()) {
                    return StreamDetails.empty(streamName);
                }

                return new StreamDetails(
                    rs.getInt(1),
                    streamName,
                    rs.getLong(2),
                    rs.getLong(3),
                    ResultSets.getLong(rs, 4),
                    ResultSets.getLong(rs, 5)
                );
            }
        }
    }

    // TODO: same as updating metadata?
    private void updateStream(Connection connection, int streamId, long version, long position) throws SQLException {
        try (PreparedStatement updateStreamsStmt = connection.prepareStatement(scripts.updateStream())) {
            updateStreamsStmt.setLong(1, version);
            updateStreamsStmt.setLong(2, position);
            updateStreamsStmt.setInt(3, streamId);
            updateStreamsStmt.executeUpdate();
            // TODO: verify update
        }
    }

    // TODO: should this return null?
    private StreamMessage getLastStreamMessage(Connection connection, String streamName) throws SQLException {
        final ReadStreamPage page = readStreamInternal(
            streamName,
            StreamVersion.END,
            1,
            ReadDirection.BACKWARD,
            true,
            null,
            connection);

        if (page.getStatus() == PageReadStatus.STREAM_NOT_FOUND) {
            return null;
        }

        return page.getMessages()[0];
    }

    private StreamDetails getOrCreateStreamDetail(Connection connection, String streamName) throws SQLException {
        StreamDetails details = getStreamDetails(connection, streamName);
        if (!details.isEmpty()) {
            return details;
        }

        return createStream(connection, streamName);
    }

    private StreamDetails createStream(Connection connection, String streamName) throws SQLException {
        try (PreparedStatement insertStmt = connection.prepareStatement(scripts.insertStream(), Statement.RETURN_GENERATED_KEYS)) {
            insertStmt.setString(1, streamName);
            insertStmt.executeUpdate();

            try (ResultSet rs = insertStmt.getGeneratedKeys()) {
                if (!rs.next()) {
                    // TODO: custom exception
                    throw new RuntimeException("failed to create stream");
                }

                int id = rs.getInt(1);
                return new StreamDetails(id, streamName, -1, -1, null, null);
            }

        }
    }

}

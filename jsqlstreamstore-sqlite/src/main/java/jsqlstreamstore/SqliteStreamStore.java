package jsqlstreamstore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import jsqlstreamstore.common.ResultSets;
import jsqlstreamstore.infrastructure.Empty;
import jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import jsqlstreamstore.store.ConnectionFactory;
import jsqlstreamstore.store.StreamStoreBase;
import jsqlstreamstore.streams.*;
import jsqlstreamstore.subscriptions.AllStreamMessageReceived;
import jsqlstreamstore.subscriptions.AllStreamSubscription;
import jsqlstreamstore.subscriptions.AllSubscriptionDropped;
import jsqlstreamstore.subscriptions.HasCaughtUp;
import jsqlstreamstore.subscriptions.StreamMessageReceived;
import jsqlstreamstore.subscriptions.StreamSubscription;
import jsqlstreamstore.subscriptions.SubscriptionDropped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

public class SqliteStreamStore extends StreamStoreBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqliteStreamStore.class);

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
    protected int getStreamMessageCount(String streamName) throws SQLException {
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(scripts.getStreamMessageCount())) {
            stmt.setString(1, streamName);
            try (ResultSet result = stmt.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    @Override
    protected AppendResult appendToStreamInternal(String streamName, long expectedVersion, NewStreamMessage[] messages) throws SQLException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName));
        Preconditions.checkArgument(expectedVersion >= -2);
        Preconditions.checkNotNull(messages);

        try (Connection connection = connectionFactory.openConnection()) {
            AppendResult result = appendToStreamInternal(connection, streamName, expectedVersion, messages);

            if (result.getMaxCount() != null) {
                checkStreamMaxCount(streamName, result.getMaxCount());
            }

            return new AppendResult(result.getCurrentVersion(), result.getCurrentPosition());
        }
    }

    private AppendResult appendToStreamInternal(Connection connection,
                                                String streamName,
                                                long expectedVersion,
                                                NewStreamMessage[] messages) throws SQLException {

        // SqlStreamStore - when returned max count is not null
        // it does CheckStreamMaxCount
        // TODO: wrap in logic to retry if deadlock
        if (expectedVersion == ExpectedVersion.ANY) {
            return appendToStreamExpectedVersionAny(
                connection,
                streamName,
                messages);
        }
        if (expectedVersion == ExpectedVersion.NO_STREAM) {
            return appendToStreamExpectedVersionNoStream(
                connection,
                streamName,
                messages);
        }
        return appendToStreamExpectedVersion(
            connection,
            streamName,
            expectedVersion,
            messages);
    }


    private AppendResult appendToStreamExpectedVersionAny(Connection connection,
                                                          String streamName,
                                                          NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try {
            try (ResultSet rs = insert(connection, streamName, messages)) {
                rs.next();
                // TODO: get max count
                return new AppendResult(null, rs.getInt(2), rs.getInt(3));
            }
        } catch (SQLException ex) {
            // TODO: review this
            connection.rollback();
            if (ex instanceof SQLiteException) {
                SQLiteException sqLiteException = (SQLiteException) ex;
                if (SQLITE_CONSTRAINT_UNIQUE == sqLiteException.getResultCode())    {
                    ReadStreamPage page = readStreamInternal(
                        streamName,
                        StreamVersion.START,
                        messages.length,
                        ReadDirection.FORWARD,
                        false,
                        null,
                        connection);

                    if (messages.length > page.getMessages().length) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                            ex);
                    }

                    for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                        if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                            throw new WrongExpectedVersion(
                                ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                                ex);
                        }
                    }
                    return new AppendResult(
                        null,
                        page.getLastStreamVersion(),
                        page.getLastStreamPosition());
                }
            }

            throw ex;
        }

    }

    private AppendResult appendToStreamExpectedVersionNoStream(Connection connection,
                                                               String streamName,
                                                               NewStreamMessage[] messages) throws SQLException {

        // TODO: check expected version

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try {
            try (ResultSet rs = insert(connection, streamName, messages)) {
                rs.next();
                return new AppendResult(null, rs.getInt(3), rs.getInt(4));
            }
        } catch (SQLException ex) {
            // might be better to use idempotent write in sql script such as SqlStreamStore postgres
            connection.rollback();

            if (ex instanceof SQLiteException) {
                SQLiteException sqLiteException = (SQLiteException) ex;
                if (SQLITE_CONSTRAINT_UNIQUE == sqLiteException.getResultCode())    {
                    ReadStreamPage page = readStreamInternal(
                        streamName,
                        StreamVersion.START,
                        messages.length,
                        ReadDirection.FORWARD,
                        false,
                        null,
                        connection);

                    if (messages.length > page.getMessages().length) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                            ex);
                    }

                    for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                        if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                            throw new WrongExpectedVersion(
                                ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                                ex);
                        }
                    }
                    return new AppendResult(
                        null,
                        page.getLastStreamVersion(),
                        page.getLastStreamPosition());
                }
            }

            throw ex;
        }

    }

    private AppendResult appendToStreamExpectedVersion(Connection connection,
                                                       String streamName,
                                                       long expectedVersion,
                                                       NewStreamMessage[] messages) throws SQLException {

        // TODO: check expected version

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try {
            // TODO: insert should probably return appendresult
            try (ResultSet rs = insert(connection, streamName, messages)) {
                rs.next();

                MetadataMessage streamMetadataMessage = rs.getString(1) == null
                    ? null
                    : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);
                Long maxCount = streamMetadataMessage == null
                    ? null
                    : streamMetadataMessage.getMaxCount();

                return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
            }
        } catch (SQLException ex) {
            connection.rollback();
            if (ex instanceof SQLiteException) {
                SQLiteException sqLiteException = (SQLiteException) ex;
                if (SQLITE_CONSTRAINT_UNIQUE == sqLiteException.getResultCode())    {
                    ReadStreamPage page = readStreamInternal(
                        streamName,
                        StreamVersion.START,
                        messages.length,
                        ReadDirection.FORWARD,
                        false,
                        null,
                        connection);

                    if (messages.length > page.getMessages().length) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                            ex);
                    }

                    for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                        if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                            throw new WrongExpectedVersion(
                                ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                                ex);
                        }
                    }
                    return new AppendResult(
                        null,
                        page.getLastStreamVersion(),
                        page.getLastStreamPosition());
                }
            }

            throw ex;
        }

    }

    private ResultSet insert(Connection connection, String streamName, NewStreamMessage[] newMessages) throws SQLException {
        connection.setAutoCommit(false);
        PreparedStatement insertStreams = connection.prepareStatement(scripts.insertStream());
        insertStreams.setString(1, streamName);
        int insertStreamsResult = insertStreams.executeUpdate();
        // TODO: check result


        // TODO: make a sql script
        String latestStreamPositionSql =
            "SELECT streams.id, streams.version, streams.position "
                + "FROM streams "
                + "WHERE streams.name = ?;";


        // TODO: should be closing resultsets
        PreparedStatement latestStreamPositionStmt = connection.prepareStatement(latestStreamPositionSql);
        latestStreamPositionStmt.setString(1, streamName);
        ResultSet latestStreamPositionResult = latestStreamPositionStmt.executeQuery();
        Integer streamIdInternal = null;
        int latestStreamVersionNewMessages = 0;
        int latestStreamPosition = 0;
        if (latestStreamPositionResult.next()) {
            streamIdInternal = latestStreamPositionResult.getInt(1);
            latestStreamVersionNewMessages = latestStreamPositionResult.getInt(2);
            latestStreamPosition = latestStreamPositionResult.getInt(3);
            if (latestStreamPositionResult.wasNull()) {
                latestStreamPosition = -1;
            }
        }

        // Then insert...
        batchInsert(connection, streamIdInternal, newMessages, latestStreamVersionNewMessages);

        String streamVersion = "SELECT version, position "
            + "FROM messages "
            + "WHERE messages.stream_id = ? "
            + "ORDER BY messages.position DESC "
            + "LIMIT 1";
        PreparedStatement streamVersionQuery = connection.prepareStatement(streamVersion);
        streamVersionQuery.setInt(1, streamIdInternal);
        ResultSet streamVersionQueryResult = streamVersionQuery.executeQuery();
        int latestStreamVersion = 0;
        if (streamVersionQueryResult.next()) {
            latestStreamVersion = streamVersionQueryResult.getInt(1);
        }

        updateStream(connection, streamIdInternal, latestStreamVersion, latestStreamPosition);

        // TODO: make compatible with postgres
        String selectLastMessageSql = "SELECT data, metadata, version, position "
            + "FROM messages "
            + "WHERE messages.stream_id = ? "
            + "ORDER BY messages.position DESC "
            + "LIMIT 1";
        PreparedStatement selectLastMessageStmt = connection.prepareStatement(selectLastMessageSql);
        selectLastMessageStmt.setInt(1, streamIdInternal);

        // TODO: move commit up?
        connection.commit();

        return selectLastMessageStmt.executeQuery();
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
                ps.setInt(3, i + 1);
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

    private static String quoteWrap(String s) {
        return "\"" + s + "\"";
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
            appendToStreamExpectedVersionAny(connection, Deleted.DELETED_STREAM_ID, new NewStreamMessage[] { streamDeletedEvent });

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
            appendToStreamExpectedVersionAny(connection, Deleted.DELETED_STREAM_ID, new NewStreamMessage[] { streamDeletedEvent });
        }

    }

    @Override
    protected void deleteMessageInternal(String streamName, UUID messageId) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);

            int streamId = getInternalStreamId(connection, streamName);

            try (PreparedStatement stmt = connection.prepareStatement(scripts.deleteStreamMessage())) {
                stmt.setInt(1, streamId);
                stmt.setObject(2, messageId);

                stmt.execute();
                if (stmt.getUpdateCount() == 1) {
                    NewStreamMessage deletedMessage = Deleted.createMessageDeletedMessage(streamName, messageId);
                    appendToStreamExpectedVersionAny(connection, Deleted.DELETED_STREAM_ID, new NewStreamMessage[] { deletedMessage });
                }
            }
            connection.commit();
        }
    }

    private int getInternalStreamId(Connection connection, String streamName) throws SQLException {

        StreamDetails details = getStreamDetails(connection, streamName);
        return details.getId();

        // TODO: do I want to keep this or just remove?
//        try (Connection connection = connectionFactory.openConnection()) {
//            try (PreparedStatement stmt = connection.prepareStatement(scripts.getInternalStreamId())) {
//                stmt.setString(1, streamName);
//                try (ResultSet rs = stmt.executeQuery()) {
//                    if (rs.next()) {
//                        return rs.getString(1);
//                    }
//
//                    return null;
//                }
//            }
//        }
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
                                                  long maxCount,
                                                  boolean prefetch,
                                                  ReadNextAllPage readNextAllPage) throws SQLException {
        long ordinal = fromPositionExclusive;

        int resultSetCount;
        String commandText = prefetch ? scripts.readAllForwardWithData() : scripts.readAllForward();
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(commandText)) {

            stmt.setLong(1, ordinal);
            // Read extra row to see if at end or not
            stmt.setLong(2, maxCount == Long.MAX_VALUE ? maxCount : maxCount + 1);

            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    if (messages.size() < maxCount) {
                        int streamId = result.getInt(1);
                        String streamName = result.getString(2);
                        int streamVersion = result.getInt(3);
                        ordinal = result.getLong(4);
                        UUID messageId = UUID.fromString(result.getString(5));
                        // TODO: I dont think we need or want this
                        LocalDateTime created = LocalDateTime.parse(result.getString(6));
                        String type = result.getString(7);
                        String jsonMetadata = result.getString(8);

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
                                jsonMetadata,
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
                                jsonMetadata,
                                () -> getJsonData(streamId, streamVersion)
                            );
                        }


                        messages.add(message);
                    }
                }
                resultSetCount = result.getRow();
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
                                                   long maxCount,
                                                   boolean prefetch,
                                                   ReadNextAllPage readNextAllPage) throws SQLException {
        long ordinal = fromPositionExclusive == Position.END ? Long.MAX_VALUE : fromPositionExclusive;

        int resultSetCount;
        String commandText = prefetch ? scripts.readAllBackwardWithData() : scripts.readAllBackward();
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(commandText)) {

            stmt.setLong(1, ordinal);
            // Read extra row to see if at end or not
            stmt.setLong(2, maxCount == Long.MAX_VALUE ? maxCount : maxCount + 1);

            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    if (messages.size() < maxCount) {
                        int streamId = result.getInt(1);
                        String streamName = result.getString(2);
                        int streamVersion = result.getInt(3);
                        ordinal = result.getLong(4);
                        UUID messageId = UUID.fromString(result.getString(5));
                        // TODO: I dont think we need or want this
                        LocalDateTime created = LocalDateTime.parse(result.getString(6));
                        String type = result.getString(7);
                        String jsonMetadata = result.getString(8);

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
                                jsonMetadata,
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
                                jsonMetadata,
                                () -> getJsonData(streamId, streamVersion)
                            );
                        }

                        messages.add(message);
                    }
                }

                resultSetCount = result.getRow();
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

            long nextPosition = Iterables.getLast(messages).getPosition() + 1;
            return new ReadAllPage(
                fromPositionExclusive,
                nextPosition,
                maxCount >= resultSetCount,
                ReadDirection.BACKWARD,
                readNextAllPage,
                messages.toArray(Empty.STREAM_MESSAGE));
        }
    }

    @Override
    protected ReadStreamPage readStreamForwardsInternal(String streamName,
                                                        long start,
                                                        long count,
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
                                                         long count,
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
            // TODO: use constant
            return result.wasNull() ? -1L : result.getLong(1);
        } catch (SQLException ex) {
            // TODO: do we want to throw SqlException?
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected StreamMetadataResult getStreamMetadataInternal(String streamName) throws SQLException {
        ReadStreamPage page;
        try (Connection connection = connectionFactory.openConnection()) {
            page = readStreamInternal(
                streamName,
                StreamVersion.END,
                1,
                ReadDirection.BACKWARD,
                true,
                null,
                connection);
        }

        if (page.getStatus() == PageReadStatus.STREAM_NOT_FOUND) {
            return new StreamMetadataResult(streamName, -1);
        }

        MetadataMessage metadataMessage = jsonSerializerStrategy.fromJson(page.getMessages()[0].getJsonData(), MetadataMessage.class);
        return new StreamMetadataResult(
            streamName,
            page.getLastStreamVersion(),
            metadataMessage.getMaxAge(),
            metadataMessage.getMaxCount(),
            metadataMessage.getMetaJson());
    }

    // TODO: make async/reactive
    private void checkStreamMaxCount(String streamId, Long maxCount) throws SQLException {
        if (maxCount != null) {
            Integer count = getStreamMessageCount(streamId);
            if (count > maxCount) {
                long toPurge = count - maxCount;
                ReadStreamPage streamMessagesPage = readStreamForwardsInternal(
                    streamId,
                    StreamVersion.START,
                    toPurge,
                    false,
                    null);

                if (streamMessagesPage.getStatus() == PageReadStatus.SUCCESS) {
                    for (StreamMessage message : streamMessagesPage.getMessages()) {
                        deleteMessageInternal(streamId, message.getMessageId());
                    }
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
        } catch (Exception ex) {
            LOG.error("", ex);
            throw new RuntimeException(ex);
        }
    }


    // TODO: make this reactive/async?
    private String getJsonData(int streamId, long streamVersion) throws SQLException {
        String commandText = scripts.readMessageData();
        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement stmt = connection.prepareStatement(commandText)) {
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

            ResultSet rs = stmt.executeQuery();

            // TODO: should we return null instead?
            if (!rs.next()) {
                throw new StreamNotFoundException(streamName);
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

    // TODO: same as updating metadata?
    private void updateStream(Connection connection, int streamId, long version, long position) throws SQLException {
        try (PreparedStatement updateStreamsStmt = connection.prepareStatement(scripts.updateStream())) {
            updateStreamsStmt.setLong(1, version);
            updateStreamsStmt.setLong(2, position);
            updateStreamsStmt.setInt(3, streamId);
            updateStreamsStmt.executeUpdate();
        }
    }

    // TODO: should we just use readHead/getLastMessage???
    private int getStreamVersionOfMessageId(Connection connection,
                                            String streamName,
                                            UUID messageId) throws SQLException {

        try (PreparedStatement command = connection.prepareStatement(scripts.getStreamVersionOfMessageId())) {
            command.setString(1, streamName);
            command.setString(2, messageId.toString());

            command.execute();
            // TODO: check result

            try (ResultSet rs = command.getResultSet()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}

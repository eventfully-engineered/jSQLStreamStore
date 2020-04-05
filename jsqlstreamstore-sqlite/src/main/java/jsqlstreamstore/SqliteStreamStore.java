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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

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
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(scripts.getStreamMessageCount())) {
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

        try (ResultSet rs = insert(connection, streamName, messages)) {
            rs.next();
            // TODO: get max count
            return new AppendResult(null, rs.getInt(2), rs.getInt(3));
        } catch (SQLException ex) {
            // TODO: review this
            connection.rollback();
            // Should we catch a PGSqlException instead?
            // https://github.com/SQLStreamStore/SQLStreamStore/blob/f7c3045f74d14be120f0c15cb0b25c48c173f012/src/SqlStreamStore.MsSql/MsSqlStreamStore.AppendStream.cs#L177
            // SQLIntegrityConstraintViolationException
            // TODO: fix thix. should be contains but SqlCode might be better????
            if ("IX_Messages_StreamIdInternal_Id".equals(ex.getMessage())) {
                int streamVersion = getStreamVersionOfMessageId(
                    connection,
                    streamName,
                    messages[0].getMessageId());

                ReadStreamPage page = readStreamInternal(
                    streamName,
                    streamVersion,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(
                        ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.ANY),
                        ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.ANY),
                            ex);
                    }
                }
                return new AppendResult(
                    null,
                    page.getLastStreamVersion(),
                    page.getLastStreamPosition());
            }

            // TODO: fix this....doesn't seem to work. check docs
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                throw new WrongExpectedVersion(
                    ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.ANY),
                    ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private AppendResult appendToStreamExpectedVersionNoStream(Connection connection,
                                                               String streamName,
                                                               NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (ResultSet rs = insert(connection, streamName, messages)) {
            rs.next();
            return new AppendResult(null, rs.getInt(3), rs.getInt(4));
        } catch (SQLException ex) {
            // Should we catch a PGSqlException instead?
            // might be better to use idempotent write in sql script such as SqlStreamStore postgres
            connection.rollback();

            // https://github.com/SQLStreamStore/SQLStreamStore/blob/f7c3045f74d14be120f0c15cb0b25c48c173f012/src/SqlStreamStore.MsSql/MsSqlStreamStore.AppendStream.cs#L177
            // SQLIntegrityConstraintViolationException
            // 23505
            // ix_streams_id

            // TODO: this doesnt appear to work
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                LOG.error("integrity constraint violation");
            }

            if ("23505".equals(ex.getSQLState())) {
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

            // TODO: fix this....doesn't seem to work. check docs
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                throw new WrongExpectedVersion(
                    ErrorMessages.appendFailedWrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM),
                    ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private AppendResult appendToStreamExpectedVersion(Connection connection,
                                                       String streamName,
                                                       long expectedVersion,
                                                       NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (ResultSet rs = insert(connection, streamName, messages)) {
            rs.next();

            MetadataMessage streamMetadataMessage = rs.getString(1) == null ? null : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);
            Long maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

            return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
        } catch (SQLException ex) {
            connection.rollback();
            // Should we catch a PGSqlException instead?
            // https://github.com/SQLStreamStore/SQLStreamStore/blob/f7c3045f74d14be120f0c15cb0b25c48c173f012/src/SqlStreamStore.MsSql/MsSqlStreamStore.AppendStream.cs#L372
            if ("WrongExpectedVersion".equals(ex.getMessage())) {
                ReadStreamPage page = readStreamInternal(
                    streamName,
                    expectedVersion + 1,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(
                        ErrorMessages.appendFailedWrongExpectedVersion(streamName, expectedVersion),
                        ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(streamName, expectedVersion),
                            ex);
                    }
                }
                return new AppendResult(
                    null,
                    page.getLastStreamVersion(),
                    page.getLastStreamPosition());
            }

            // TODO: fix this...this
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                throw new WrongExpectedVersion(
                    ErrorMessages.appendFailedWrongExpectedVersion(streamName, expectedVersion),
                    ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private ResultSet insert(Connection connection, String streamName, NewStreamMessage newMessages[]) throws SQLException {
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
        batchInsert(streamIdInternal, newMessages, latestStreamVersionNewMessages);

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

        PreparedStatement updateStreamsStmt = connection.prepareStatement(scripts.updateStream());
        updateStreamsStmt.setInt(1, latestStreamVersion);
        updateStreamsStmt.setInt(2, latestStreamPosition);
        updateStreamsStmt.setInt(3, streamIdInternal);
        updateStreamsStmt.executeUpdate();

        // TODO: make compatible with postgres
        String selectLastMessageSql = "SELECT data, metadata, version, position "
            + "FROM messages "
            + "WHERE messages.stream_id = ? "
            + "ORDER BY messages.position DESC "
            + "LIMIT 1";
        PreparedStatement selectLastMessageStmt = connection.prepareStatement(selectLastMessageSql);
        selectLastMessageStmt.setInt(1, streamIdInternal);

        connection.commit();

        return selectLastMessageStmt.executeQuery();
    }

    private class InsertResult {
        private String jsonData;
        private int currentVersion;
        private int currentPosition;

        public InsertResult(String jsonData, int currentVersion, int currentPosition) {
            this.jsonData = jsonData;
            this.currentVersion = currentVersion;
            this.currentPosition = currentPosition;
        }
    }


    private void batchInsert(Integer streamInternal, NewStreamMessage[] messages, long latestStreamVersion) throws SQLException {
        // TODO: just use the table default
        LocalDateTime date = LocalDateTime.now(ZoneId.of("UTC"));
        PreparedStatement ps = connectionFactory.openConnection().prepareStatement(scripts.writeMessage());
        final int batchSize = 1000;
        int count = 0;
        for (int i = 0; i < messages.length; i++) {
            NewStreamMessage message = messages[i];
            ps.setString(1, message.getMessageId().toString());
            ps.setInt(2, streamInternal);
            ps.setString(3, String.valueOf(i + 1));
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
        ps.close();
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
    protected void deleteMessageInternal(String streamId, UUID messageId) throws SQLException {
        // TODO: could we batch the delete and inserts?
        // TODO: SQLStreamStore also does this via a taskqueue
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);

            String internalStreamId = getInternalStreamId(streamId);
            // TODO: handle null

            try (PreparedStatement stmt = connection.prepareStatement(scripts.deleteStreamMessage())) {
                SqlStreamId sqlStreamId = new SqlStreamId(streamId);

                stmt.setString(1, internalStreamId);
                stmt.setObject(2, messageId);

                stmt.execute();
                if (stmt.getUpdateCount() == 1) {
                    NewStreamMessage deletedMessage = Deleted.createMessageDeletedMessage(sqlStreamId.getOriginalId(), messageId);
                    appendToStreamExpectedVersionAny(connection, Deleted.DELETED_STREAM_ID, new NewStreamMessage[] { deletedMessage });
                }
            }
            connection.commit();
        }
    }

    private String getInternalStreamId(String streamId) throws SQLException {
        // TODO: could we batch the delete and inserts?
        // TODO: SQLStreamStore also does this via a taskqueue
        try (Connection connection = connectionFactory.openConnection()) {
            try (PreparedStatement stmt = connection.prepareStatement(scripts.getInternalStreamId())) {
                SqlStreamId sqlStreamId = new SqlStreamId(streamId);

                stmt.setString(1, sqlStreamId.getId());

                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    return rs.getString(1);
                }

                return null;
            }
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
                                                  long maxCount,
                                                  boolean prefetch,
                                                  ReadNextAllPage readNextAllPage) throws SQLException {
        long ordinal = fromPositionExclusive;

        int resultSetCount;
        String commandText = prefetch ? scripts.readAllForwardWithData() : scripts.readAllForward();
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(commandText)) {
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
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(commandText)) {
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

        PreparedStatement streamLatestStmt = connection.prepareStatement(scripts.getLatestFromStreamByStreamId());
        streamLatestStmt.setString(1, streamName);
        ResultSet streamLatest = streamLatestStmt.executeQuery();
        String internalId = null;
        int latestVersion = 0;
        int latestPosition = 0;
        if (streamLatest.next()) {
            internalId = streamLatest.getString(1);
            latestVersion = streamLatest.getInt(2);
            latestPosition = streamLatest.getInt(3);
        }

        try (PreparedStatement stmt = connection.prepareStatement(commandText)) {
            connection.setAutoCommit(false);

            stmt.setString(1, internalId);
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
                        LocalDateTime created = LocalDateTime.from(DateTimeFormatter.ISO_DATE_TIME.parse(result.getString(5)));
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
                getNextVersion.get(messages, latestVersion),
                latestVersion,
                latestPosition,
                direction,
                count >= resultSetCount,
                readNext,
                messages.toArray(Empty.STREAM_MESSAGE));
        } catch (Exception ex) {
            LOG.error("", ex);
            throw new RuntimeException(ex);
        }
    }


    // TODO: make this reactive/async?
    private String getJsonData(int streamId, long streamVersion) throws SQLException {
        String commandText = scripts.readMessageData();
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(commandText)) {
            stmt.setInt(1, streamId);
            stmt.setLong(2, streamVersion);

            try (ResultSet result = stmt.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }

        }
    }

    // TODO: should we just use readHead/getLastMessage???
    private int getStreamVersionOfMessageId(Connection connection,
                                            String streamName,
                                            UUID messageId) throws SQLException {

        try (PreparedStatement command = connection.prepareStatement(scripts.getStreamVersionOfMessageId())) {
            connection.setAutoCommit(false);

            command.setString(1, streamName);
            command.setString(2, messageId.toString());

            command.execute();

            try (ResultSet rs = command.getResultSet()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}

package jsqlstreamstore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
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
    protected int getStreamMessageCount(String streamId) throws SQLException {
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(scripts.getStreamMessageCount())) {
            stmt.setString(1, streamId);
            try (ResultSet result = stmt.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    @Override
    protected AppendResult appendToStreamInternal(String streamId, int expectedVersion, NewStreamMessage[] messages) throws SQLException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamId));
        Preconditions.checkArgument(expectedVersion >= -2);
        Preconditions.checkNotNull(messages);

        try (Connection connection = connectionFactory.openConnection()) {
            SqlStreamId sqlStreamId = new SqlStreamId(streamId);
            AppendResult result = appendToStreamInternal(connection, sqlStreamId, expectedVersion, messages);

            if (result.getMaxCount() != null) {
                checkStreamMaxCount(streamId, result.getMaxCount());
            }

            return new AppendResult(result.getCurrentVersion(), result.getCurrentPosition());
        }
    }

    private AppendResult appendToStreamInternal(Connection connection,
                                                SqlStreamId sqlStreamId,
                                                int expectedVersion,
                                                NewStreamMessage[] messages) throws SQLException {

        // SqlStreamStore - when returned max count is not null
        // it does CheckStreamMaxCount
        // TODO: wrap in logic to retry if deadlock
        if (expectedVersion == ExpectedVersion.ANY) {
            return appendToStreamExpectedVersionAny(
                connection,
                sqlStreamId,
                messages);
        }
        if (expectedVersion == ExpectedVersion.NO_STREAM) {
            return appendToStreamExpectedVersionNoStream(
                connection,
                sqlStreamId,
                messages);
        }
        return appendToStreamExpectedVersion(
            connection,
            sqlStreamId,
            expectedVersion,
            messages);
    }


    private AppendResult appendToStreamExpectedVersionAny(Connection connection,
                                                          SqlStreamId sqlStreamId,
                                                          NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (ResultSet rs = insert(connection, sqlStreamId.getId(), sqlStreamId.getOriginalId(), messages)) {
            rs.next();
            MetadataMessage streamMetadataMessage = rs.getString(1) == null
                ? null
                : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);

            Integer maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

            return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
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
                    sqlStreamId,
                    messages[0].getMessageId());

                ReadStreamPage page = readStreamInternal(
                    sqlStreamId,
                    streamVersion,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(
                        ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), ExpectedVersion.ANY),
                        ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), ExpectedVersion.ANY),
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
                    ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), ExpectedVersion.ANY),
                    ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private AppendResult appendToStreamExpectedVersionNoStream(Connection connection,
                                                               SqlStreamId sqlStreamId,
                                                               NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (ResultSet rs = insert(connection, sqlStreamId.getId(), sqlStreamId.getOriginalId(), messages)) {
            rs.next();

            MetadataMessage streamMetadataMessage = rs.getString(2) == null ? null : jsonSerializerStrategy.fromJson(rs.getString(2), MetadataMessage.class);
            Integer maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

            return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
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
                    sqlStreamId,
                    StreamVersion.START,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(
                        ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), ExpectedVersion.NO_STREAM),
                        ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), ExpectedVersion.NO_STREAM),
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
                    ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), ExpectedVersion.NO_STREAM),
                    ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private AppendResult appendToStreamExpectedVersion(Connection connection,
                                                       SqlStreamId sqlStreamId,
                                                       int expectedVersion,
                                                       NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (ResultSet rs = insert(connection, sqlStreamId.getId(), sqlStreamId.getOriginalId(), messages)) {
            rs.next();

            MetadataMessage streamMetadataMessage = rs.getString(1) == null ? null : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);
            Integer maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

            return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
        } catch (SQLException ex) {
            connection.rollback();
            // Should we catch a PGSqlException instead?
            // https://github.com/SQLStreamStore/SQLStreamStore/blob/f7c3045f74d14be120f0c15cb0b25c48c173f012/src/SqlStreamStore.MsSql/MsSqlStreamStore.AppendStream.cs#L372
            if ("WrongExpectedVersion".equals(ex.getMessage())) {
                ReadStreamPage page = readStreamInternal(
                    sqlStreamId,
                    expectedVersion + 1,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(
                        ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), expectedVersion),
                        ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(
                            ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), expectedVersion),
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
                    ErrorMessages.appendFailedWrongExpectedVersion(sqlStreamId.getOriginalId(), expectedVersion),
                    ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private ResultSet insert(Connection connection, String streamId, String streamIdOriginal, NewStreamMessage newMessages[]) throws SQLException {
        connection.setAutoCommit(false);
        //        IF NOT EXISTS (SELECT * FROM public.Streams WHERE public.Streams.Id = streamId) THEN
        //        INSERT INTO public.Streams (Id, IdOriginal) VALUES (streamId, streamIdOriginal);
        //        END IF;

        // see if stream exists...with one of these options
        // 1.
        // INSERT OR IGNORE INTO Streams(Id, IdOriginal) VALUES(streamId, streamIdOriginal)
// 2.
//        INSERT INTO Streams(Id, IdOriginal)
//        SELECT 5, 'text to insert'
//        WHERE NOT EXISTS(SELECT 1 FROM memos WHERE id = 5 AND text = 'text to insert');

        PreparedStatement insertStreams = connection.prepareStatement("INSERT INTO Streams(Id, IdOriginal) VALUES (?, ?);");
        insertStreams.setString(1, streamId);
        insertStreams.setString(2, streamIdOriginal);
        int insertStreamsResult = insertStreams.executeUpdate();
        // TODO: check result


        // TODO: make a sql script
        String latestStreamPositionSql =
            "SELECT Streams.IdInternal, Streams.\"Version\" as latestStreamVersion, Streams.\"Position\" as latestStreamPosition "
                + "FROM Streams "
                + "WHERE Streams.Id = ?";

        // TODO: should be closing resultsets
        PreparedStatement latestStreamPositionStmt = connection.prepareStatement(latestStreamPositionSql);
        latestStreamPositionStmt.setString(1, streamId);
        ResultSet latestStreamPositionResult = latestStreamPositionStmt.executeQuery();
        String streamIdInternal = null;
        int latestStreamVersionNewMessages = 0;
        int latestStreamPosition = 0;
        if (latestStreamPositionResult.next()) {
            streamIdInternal = latestStreamPositionResult.getString(1);
            latestStreamVersionNewMessages = latestStreamPositionResult.getInt(2);
            latestStreamPosition = latestStreamPositionResult.getInt(3);
            if (latestStreamPositionResult.wasNull()) {
                latestStreamPosition = -1;
            }
        }

        // Then insert...
        batchInsert(streamIdInternal, newMessages, latestStreamVersionNewMessages);

        String streamVersion = "SELECT StreamVersion, Position as latestStreamVersion "
            + "FROM Messages "
            + "WHERE Messages.StreamIDInternal = ? "
            + "ORDER BY Messages.Position DESC "
            + "LIMIT 1";
        PreparedStatement streamVersionQuery = connection.prepareStatement(streamVersion);
        streamVersionQuery.setString(1, streamIdInternal);
        ResultSet streamVersionQueryResult = streamVersionQuery.executeQuery();
        int latestStreamVersion = 0;
        if (streamVersionQueryResult.next()) {
            latestStreamVersion = streamVersionQueryResult.getInt(1);
        }

        String updateStreamsSql = "UPDATE Streams "
            + "SET \"Version\" = ?, \"Position\" = ? "
            + "WHERE Streams.IdInternal = ?";
        PreparedStatement updateStreamsStmt = connection.prepareStatement(updateStreamsSql);
        updateStreamsStmt.setInt(1, latestStreamVersion);
        updateStreamsStmt.setInt(2, latestStreamPosition);
        updateStreamsStmt.setString(3, streamIdInternal);
        updateStreamsStmt.executeUpdate();

        String selectLastMessageSql = "SELECT JsonData, JsonMetadata, StreamVersion, Position "
            + "FROM Messages "
            + "WHERE Messages.StreamIdInternal = ? "
            + "ORDER BY Messages.Position DESC "
            + "LIMIT 1";
        PreparedStatement selectLastMessageStmt = connection.prepareStatement(selectLastMessageSql);
        selectLastMessageStmt.setString(1, streamIdInternal);

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


    private void batchInsert(String streamInternal, NewStreamMessage[] messages, int latestStreamVersion) throws SQLException {
        // TODO: just use the table default
        LocalDateTime date = LocalDateTime.now(ZoneId.of("UTC"));
        String sql = "INSERT INTO Messages (StreamIdInternal, Id, StreamVersion, Created, Type, JsonData, JsonMetadata) VALUES (?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = connectionFactory.openConnection().prepareStatement(sql);
        final int batchSize = 1000;
        int count = 0;
        for (int i = 0; i < messages.length; i++) {
            NewStreamMessage message = messages[i];
            ps.setString(1, streamInternal);
            ps.setString(2, message.getMessageId().toString());
            ps.setString(3, String.valueOf(i + 1));
            ps.setString(4, date.toString());
            ps.setString(5, message.getType());
            ps.setString(6, message.getJsonData());
            ps.setString(7, message.getJsonMetadata());
            ps.addBatch();

            if (++count % batchSize == 0) {
                ps.executeBatch();
            }
        }
        // TODO: dont need to call if equal to mod batch size
        ps.executeBatch();
        ps.close();

//        Statement batch = connectionFactory.openConnection().createStatement();
//        DateTime date = DateTime.now();
//        StringBuilder valuesBuilder = new StringBuilder();
//        for (int i = 0; i < messages.length; i++) {
//            if (i != 0) {
//                valuesBuilder.append(", ");
//            }
//            // TODO: joiner probably isnt worth it
//            NewStreamMessage message = messages[i];
//            Joiner j = Joiner.on(",");
//            String s = j.join(
//                streamInternal,
//                quoteWrap(message.getMessageId().toString()),
//                String.valueOf(i + 1),
//                quoteWrap(date.toString()),
//                quoteWrap(message.getType()),
//                quoteWrap(StringEscapeUtils.escapeJson(message.getJsonData())),
//                quoteWrap(StringEscapeUtils.escapeJson(message.getJsonMetadata()))
//            );
//
//
//            // valuesBuilder.append("(").append(s).append(")");
//            valuesBuilder.append("(?, ?, ?, ?, ?, ?, ?)");
//        }
//
//        // TODO: compare with adding inserts to batch
//
//        boolean result = batch.execute("INSERT INTO Messages (StreamIdInternal, Id, StreamVersion, Created, Type, JsonData, JsonMetadata) VALUES " + valuesBuilder);
//

        // TODO: do something with result
    }

    private static String quoteWrap(String s) {
        return "\"" + s + "\"";
    }

    @Override
    protected void deleteStreamInternal(String streamId, int expectedVersion) throws SQLException {
        SqlStreamId sqlStreamId = new SqlStreamId(streamId);
        if (expectedVersion == ExpectedVersion.ANY) {
            deleteStreamAnyVersion(sqlStreamId);
        } else {
            deleteStreamExpectedVersion(sqlStreamId, expectedVersion);
        }
    }

    private void deleteStreamExpectedVersion(SqlStreamId sqlStreamId, int expectedVersion) throws SQLException {
        try (Connection connection = connectionFactory.openConnection();
             CallableStatement stmt = connection.prepareCall(scripts.deleteStreamExpectedVersion())) {

            connection.setAutoCommit(false);
            stmt.setString(1, sqlStreamId.getId());
            stmt.setInt(2, expectedVersion);
            stmt.executeUpdate();

            NewStreamMessage streamDeletedEvent = Deleted.createStreamDeletedMessage(sqlStreamId.getOriginalId());
            appendToStreamExpectedVersionAny(connection, Deleted.DELETED_SQL_STREAM_ID, new NewStreamMessage[] { streamDeletedEvent });

            // Delete metadata stream (if it exists)
            deleteStreamAnyVersion(connection, sqlStreamId.getMetadataSqlStreamId());

            connection.commit();
        }
    }

    private void deleteStreamAnyVersion(SqlStreamId sqlStreamId) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);
            deleteStreamAnyVersion(connection, sqlStreamId);
            // Delete metadata stream (if it exists)
            deleteStreamAnyVersion(connection, sqlStreamId.getMetadataSqlStreamId());
            connection.commit();
        }
    }

    private void deleteStreamAnyVersion(Connection connection, SqlStreamId sqlStreamId) throws SQLException {

        boolean aStreamIsDeleted;
        try (CallableStatement stmt = connection.prepareCall(scripts.deleteStreamAnyVersion())) {
            stmt.setString(1, sqlStreamId.getId());
            int count = stmt.executeUpdate();

            aStreamIsDeleted = count > 0;
        }

        if (aStreamIsDeleted) {
            NewStreamMessage streamDeletedEvent = Deleted.createStreamDeletedMessage(sqlStreamId.getOriginalId());
            appendToStreamExpectedVersionAny(connection, Deleted.DELETED_SQL_STREAM_ID, new NewStreamMessage[] { streamDeletedEvent });
        }

    }

    @Override
    protected void deleteMessageInternal(String streamId, UUID messageId) throws SQLException {
        // TODO: could we batch the delete and inserts?
        // TODO: SQLStreamStore also does this via a taskqueue
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);

            try (CallableStatement stmt = connection.prepareCall(scripts.deleteStreamMessage())) {
                SqlStreamId sqlStreamId = new SqlStreamId(streamId);

                stmt.setString(1, sqlStreamId.getId());
                stmt.setObject(2, messageId);

                stmt.execute();
                if (stmt.getUpdateCount() == 1) {
                    NewStreamMessage deletedMessage = Deleted.createMessageDeletedMessage(sqlStreamId.getOriginalId(), messageId);
                    appendToStreamExpectedVersionAny(connection, Deleted.DELETED_SQL_STREAM_ID, new NewStreamMessage[] { deletedMessage });
                }
            }
            connection.commit();
        }
    }

    @Override
    protected SetStreamMetadataResult setStreamMetadataInternal(String streamId, int expectedStreamMetadataVersion, Integer maxAge, Integer maxCount, String metadataJson) throws SQLException {
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

        int resultSetCount;
        String commandText = prefetch ? scripts.readAllForwardWithData() : scripts.readAllForward();
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(commandText)) {
            stmt.setLong(1, ordinal);
            // Read extra row to see if at end or not
            stmt.setInt(2, maxCount == Integer.MAX_VALUE ? maxCount : maxCount + 1);

            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    if (messages.size() < maxCount) {
                        String streamId = result.getString(1);
                        int streamVersion = result.getInt(2);
                        ordinal = result.getLong(3);
                        UUID messageId = UUID.fromString(result.getString(4));
                        // TODO: I dont think we need or want this
                        LocalDateTime created = LocalDateTime.parse(result.getString(5));
                        String type = result.getString(6);
                        String jsonMetadata = result.getString(7);

                        // TODO: improve
                        final StreamMessage message;
                        if (prefetch) {
                            message = new StreamMessage(
                                streamId,
                                messageId,
                                streamVersion,
                                ordinal,
                                created,
                                type,
                                jsonMetadata,
                                result.getString(8)
                            );
                        } else {
                            message = new StreamMessage(
                                streamId,
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
                messages.toArray(new StreamMessage[messages.size()]));
        }
    }

    @Override
    protected ReadAllPage readAllBackwardsInternal(long fromPositionExclusive,
                                                   int maxCount,
                                                   boolean prefetch,
                                                   ReadNextAllPage readNextAllPage) throws SQLException {
        long ordinal = fromPositionExclusive == Position.END ? Long.MAX_VALUE : fromPositionExclusive;

        int resultSetCount;
        String commandText = prefetch ? scripts.readAllBackwardWithData() : scripts.readAllBackward();
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(commandText)) {
            stmt.setLong(1, ordinal);
            // Read extra row to see if at end or not
            stmt.setInt(2, maxCount == Integer.MAX_VALUE ? maxCount : maxCount + 1);

            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    if (messages.size() < maxCount) {
                        String streamId = result.getString(1);
                        int streamVersion = result.getInt(2);
                        ordinal = result.getLong(3);
                        UUID messageId = (UUID) result.getObject(4);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = LocalDateTime.parse(result.getString(5));
                        String type = result.getString(6);
                        String jsonMetadata = result.getString(7);

                        // TODO: improve
                        final StreamMessage message;
                        if (prefetch) {
                            message = new StreamMessage(
                                streamId,
                                messageId,
                                streamVersion,
                                ordinal,
                                created,
                                type,
                                jsonMetadata,
                                result.getString(8)
                            );
                        } else {
                            message = new StreamMessage(
                                streamId,
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
    protected ReadStreamPage readStreamForwardsInternal(String streamId,
                                                        int start,
                                                        int count,
                                                        boolean prefetch,
                                                        ReadNextStreamPage readNext) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            SqlStreamId sqlStreamId = new SqlStreamId(streamId);
            return readStreamInternal(
                sqlStreamId,
                start,
                count,
                ReadDirection.FORWARD,
                prefetch,
                readNext,
                connection);
        }
    }

    @Override
    protected ReadStreamPage readStreamBackwardsInternal(String streamId,
                                                         int start,
                                                         int count,
                                                         boolean prefetch,
                                                         ReadNextStreamPage readNext) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            SqlStreamId sqlStreamId = new SqlStreamId(streamId);
            return readStreamInternal(
                sqlStreamId,
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
    protected StreamMetadataResult getStreamMetadataInternal(String streamId) throws SQLException {
        SqlStreamId sqlStreamId = new SqlStreamId(streamId);
        ReadStreamPage page;
        try (Connection connection = connectionFactory.openConnection()) {
            page = readStreamInternal(
                sqlStreamId.getMetadataSqlStreamId(),
                StreamVersion.END,
                1,
                ReadDirection.BACKWARD,
                true,
                null,
                connection);
        }

        if (page.getStatus() == PageReadStatus.STREAM_NOT_FOUND) {
            return new StreamMetadataResult(streamId, -1);
        }

        MetadataMessage metadataMessage = jsonSerializerStrategy.fromJson(page.getMessages()[0].getJsonData(), MetadataMessage.class);
        return new StreamMetadataResult(
            streamId,
            page.getLastStreamVersion(),
            metadataMessage.getMaxAge(),
            metadataMessage.getMaxCount(),
            metadataMessage.getMetaJson());
    }

    // TODO: make async/reactive
    private void checkStreamMaxCount(String streamId, Integer maxCount) throws SQLException {
        if (maxCount != null) {
            Integer count = getStreamMessageCount(streamId);
            if (count > maxCount) {
                int toPurge = count - maxCount;
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



    private ReadStreamPage readStreamInternal(SqlStreamId sqlStreamId,
                                              int start,
                                              int count,
                                              ReadDirection direction,
                                              boolean prefetch,
                                              ReadNextStreamPage readNext,
                                              Connection connection) throws SQLException {

        // To read backwards from end, need to use int MaxValue
        int streamVersion = start == StreamVersion.END ? Integer.MAX_VALUE : start;
        String commandText;
        GetNextVersion getNextVersion;
        if (direction == ReadDirection.FORWARD) {
            commandText = prefetch ? scripts.readStreamForwardWithData() : scripts.readStreamForward();
            getNextVersion = (List<StreamMessage> messages, int lastVersion) -> {
                if (messages != null && !messages.isEmpty()) {
                    return Iterables.getLast(messages).getStreamVersion() + 1;
                }
                return lastVersion + 1;
            };
        } else {
            commandText = prefetch ? scripts.readStreamBackwardsWithData() : scripts.readStreamBackwards();
            getNextVersion = (List<StreamMessage> messages, int lastVersion) -> {
                if (messages != null && !messages.isEmpty()) {
                    return Iterables.getLast(messages).getStreamVersion() - 1;
                }
                return lastVersion - 1;
            };
        }

        PreparedStatement streamLatestStmt = connection.prepareStatement(scripts.getLatestFromStreamByStreamId());
        streamLatestStmt.setString(1, sqlStreamId.getId());
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
            stmt.setInt(2, streamVersion);
            stmt.setInt(3, count == Integer.MAX_VALUE ? count : count + 1);

            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return new ReadStreamPage(
                    sqlStreamId.getOriginalId(),
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


            int lastStreamVersion = rs.getInt(1);
            // streamNotFound page
            if (rs.wasNull()) {
                return new ReadStreamPage(
                    sqlStreamId.getOriginalId(),
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
            long lastStreamPosition = rs.getLong(2);

            int resultSetCount = 0;
            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = (ResultSet) rs.getObject(3)) {

                while (result.next()) {
                    if (messages.size() < count) {
                        int streamVersion1 = result.getInt(1);
                        long ordinal = result.getLong(2);
                        UUID messageId = (UUID) result.getObject(3);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = LocalDateTime.parse(result.getString(4));
                        String type = result.getString(5);
                        String jsonMetadata = result.getString(6);

                        // TODO: improve
                        final StreamMessage message;
                        if (prefetch) {
                            message = new StreamMessage(
                                sqlStreamId.getOriginalId(),
                                messageId,
                                streamVersion1,
                                ordinal,
                                created,
                                type,
                                jsonMetadata,
                                result.getString(7)
                            );

                        } else {
                            message = new StreamMessage(
                                sqlStreamId.getOriginalId(),
                                messageId,
                                streamVersion1,
                                ordinal,
                                created,
                                type,
                                jsonMetadata,
                                () -> getJsonData(sqlStreamId.getOriginalId(), streamVersion)
                            );
                        }

                        messages.add(message);
                    }
                    resultSetCount = result.getRow();
                }
            }

            return new ReadStreamPage(
                sqlStreamId.getOriginalId(),
                PageReadStatus.SUCCESS,
                start,
                getNextVersion.get(messages, lastStreamVersion),
                lastStreamVersion,
                lastStreamPosition,
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
    private String getJsonData(String streamId, int streamVersion) throws SQLException {
        String commandText = scripts.readMessageData();
        try (PreparedStatement stmt = connectionFactory.openConnection().prepareStatement(commandText)) {
            stmt.setString(1, streamId);
            stmt.setInt(2, streamVersion);

            try (ResultSet result = stmt.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }

        }
    }

    // TODO: should we just use readHead/getLastMessage???
    private int getStreamVersionOfMessageId(Connection connection,
                                            SqlStreamId sqlStreamId,
                                            UUID messageId) throws SQLException {

        try (PreparedStatement command = connection.prepareStatement(scripts.getStreamVersionOfMessageId())) {
            connection.setAutoCommit(false);

            command.setString(1, sqlStreamId.getId());
            command.setString(2, messageId.toString());

            command.execute();

            try (ResultSet rs = command.getResultSet()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }




//    /**
//     * SQL-escape a string.
//     */
//    public static String sqlEscapeString(String value) {
//        StringBuilder escaper = new StringBuilder();
//
//        DatabaseUtils.appendEscapedSQLString(escaper, value);
//
//        return escaper.toString();
//    }
//
//    public static void appendEscapedSQLString(StringBuilder sb, String sqlString) {
//        sb.append('\'');
//        if (sqlString.indexOf('\'') != -1) {
//            int length = sqlString.length();
//            for (int i = 0; i < length; i++) {
//                char c = sqlString.charAt(i);
//                if (c == '\'') {
//                    sb.append('\'');
//                }
//                sb.append(c);
//            }
//        } else
//            sb.append(sqlString);
//        sb.append('\'');
//    }


}

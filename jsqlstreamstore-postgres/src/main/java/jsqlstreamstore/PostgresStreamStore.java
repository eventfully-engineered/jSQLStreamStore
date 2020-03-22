package jsqlstreamstore;

import com.fasterxml.uuid.Generators;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.reactivex.ObservableSource;
import jsqlstreamstore.infrastructure.Empty;
import jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import jsqlstreamstore.store.ConnectionFactory;
import jsqlstreamstore.store.StreamStoreBase;
import jsqlstreamstore.streams.*;
import jsqlstreamstore.subscriptions.*;
import org.apache.commons.text.StringEscapeUtils;

import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

// TODO: double check data types: int vs Integer vs long vs Long
public class PostgresStreamStore extends StreamStoreBase {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresStreamStore.class);

    private final ConnectionFactory connectionFactory;
    private final Scripts scripts;
    private final Supplier<StreamStoreNotifier> streamStoreNotifier;
    private final JsonSerializerStrategy jsonSerializerStrategy;

    public PostgresStreamStore(PostgresStreamStoreSettings settings) {
        super(settings.getMetadataMaxAgeCacheExpire(), settings.getMetadataMaxAgeCacheMaxSize());

        connectionFactory = settings.getConnectionFactory();
        jsonSerializerStrategy = settings.getJsonSerializerStrategy();

        streamStoreNotifier = () -> {
            if (settings.getCreateStreamStoreNotifier() == null) {
                throw new RuntimeException("Cannot create notifier because supplied createStreamStoreNotifier was null");
            }
            return settings.getCreateStreamStoreNotifier().createStreamStoreNotifier(this);
        };

        scripts = new Scripts(settings.getSchema());
    }

    @Override
    public ReadAllPage readAllForwardsInternal(long fromPositionExclusive, int maxCount, boolean prefetch, ReadNextAllPage readNextAllPage) throws SQLException {
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
                        UUID messageId = (UUID) result.getObject(4);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = result.getTimestamp(5).toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
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
    public ReadAllPage readAllBackwardsInternal(long fromPositionExclusive,
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
                        LocalDateTime created = result.getTimestamp(5).toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
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
    public ReadStreamPage readStreamForwardsInternal(String streamId,
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
    public ReadStreamPage readStreamBackwardsInternal(String streamId,
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
    public Long readHeadPositionInternal() {
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
    public StreamMetadataResult getStreamMetadataInternal(String streamId) throws SQLException {
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
    protected AppendResult appendToStreamInternal(String streamId, int expectedVersion, NewStreamMessage[] messages)
            throws SQLException {
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

    // Deadlocks appear to be a fact of life when there is high contention on a table regardless of transaction isolation settings.
    private static <T> T retryOnDeadLock(Callable<T> operation) throws Exception {
        // TODO too much? too little? configurable?
        int maxRetries = 2;
        Exception exception = null;

        int retryCount = 0;
        do {
            try {
                return operation.call();
            } catch(SQLException ex) {
                // Deadlock error codes;
                if (ex.getErrorCode() == 1205 || ex.getErrorCode() == 1222) {
                    exception = ex;
                    retryCount++;
                }
            }
        } while(retryCount < maxRetries);

        throw exception;
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

    private AppendResult appendToStreamExpectedVersionAny(Connection connection,
                                                          SqlStreamId sqlStreamId,
                                                          NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (CallableStatement cstmt = connection.prepareCall(scripts.getAppendStreamExpectedVersionAny())) {
            connection.setAutoCommit(false);

            cstmt.setString(1, sqlStreamId.getId());
            cstmt.setString(2, sqlStreamId.getOriginalId());

            // create new message type array
            PGobject[] objects = convertToPGObjectArray(messages);
            Array array = connection.createArrayOf("NewMessage", objects);
            cstmt.setObject(3, array, Types.ARRAY);

            cstmt.execute();
            connection.commit();

            try (ResultSet rs = cstmt.getResultSet()) {
                rs.next();

                MetadataMessage streamMetadataMessage = rs.getString(1) == null
                    ? null
                    : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);

                Integer maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

                return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
            }

        } catch (SQLException ex) {
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

    private int getStreamVersionOfMessageId(Connection connection,
                                            SqlStreamId sqlStreamId,
                                            UUID messageId) throws SQLException {

        try (CallableStatement command = connection.prepareCall(scripts.getStreamVersionOfMessageId())) {
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

    private AppendResult appendToStreamExpectedVersionNoStream(Connection connection,
                                                               SqlStreamId sqlStreamId,
                                                               NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (CallableStatement cstmt = connection.prepareCall(scripts.getAppendStreamExpectedVersionNoStream())) {
            connection.setAutoCommit(false);
            cstmt.setString(1, sqlStreamId.getId());
            cstmt.setString(2, sqlStreamId.getOriginalId());

            // create new message type array
            PGobject[] objects = convertToPGObjectArray(messages);
            Array array = connection.createArrayOf("NewMessage", objects);
            cstmt.setObject(3, array, Types.ARRAY);

            boolean executeResult = cstmt.execute();
            connection.commit();

            try (ResultSet rs = cstmt.getResultSet()) {
                rs.next();

                MetadataMessage streamMetadataMessage = rs.getString(1) == null ? null : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);
                Integer maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

                return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
            }
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
                System.out.println("integrity constraint violation");
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

    private static PGobject[] convertToPGObjectArray(NewStreamMessage[] messages) throws SQLException {
        // create new message type array
        PGobject[] objects = new PGobject[messages.length];
        int i = 0;
        // TODO: remove date or make it user supplied
        LocalDateTime date = LocalDateTime.now(ZoneId.of("UTC"));
        for (NewStreamMessage message : messages) {
            PGobject pgObject = new PGobject();
            pgObject.setType("NewMessage");

            // TODO: fix this...it sucks
            Joiner j = Joiner.on(",");
            String s = j.join(message.getMessageId().toString(), String.valueOf(i + 1), date.toString(), message.getType(), "\""+ StringEscapeUtils.escapeJson(message.getJsonData())+"\"", "\""+StringEscapeUtils.escapeJson(message.getJsonMetadata())+"\"");
            pgObject.setValue("(" +  s + ")");
            objects[i] = pgObject;
            i++;
        }
        return objects;
    }

    private AppendResult appendToStreamExpectedVersion(Connection connection,
                                                       SqlStreamId sqlStreamId,
                                                       int expectedVersion,
                                                       NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (CallableStatement cstmt = connection.prepareCall(scripts.getAppendStreamExpectedVersion())) {
            connection.setAutoCommit(false);

            cstmt.setString(1, sqlStreamId.getId());
            cstmt.setInt(2, expectedVersion);

            // create new message type array
            PGobject[] objects = convertToPGObjectArray(messages);
            Array array = connection.createArrayOf("NewMessage", objects);
            cstmt.setObject(3, array, Types.ARRAY);

            cstmt.execute();
            connection.commit();

            try (ResultSet rs = cstmt.getResultSet()) {
                rs.next();

                MetadataMessage streamMetadataMessage = rs.getString(1) == null ? null : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);
                Integer maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

                return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
            }

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
    protected SetStreamMetadataResult setStreamMetadataInternal(String streamId,
                                                                int expectedStreamMetadataVersion,
                                                                Integer maxAge,
                                                                Integer maxCount,
                                                                String metadataJson) throws SQLException {
        AppendResult result;
        try (Connection connection = connectionFactory.openConnection()) {
            SqlStreamId sqlStreamId = new SqlStreamId(streamId);
            MetadataMessage metadataMessage = new MetadataMessage(
                streamId,
                maxAge,
                maxCount,
                metadataJson);
            String json = metadataMessage.toString();
            NewStreamMessage newMessage = new NewStreamMessage(Generators.timeBasedGenerator().generate(), "$stream-metadata", json);

            result = appendToStreamInternal(
                connection,
                sqlStreamId.getMetadataSqlStreamId(),
                expectedStreamMetadataVersion,
                new NewStreamMessage[]{newMessage});

            // TODO: reactive/task
            checkStreamMaxCount(streamId, maxCount);

            return new SetStreamMetadataResult(result.getCurrentVersion());
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

        try (CallableStatement cstmt = connection.prepareCall(commandText)) {
            connection.setAutoCommit(false);

            cstmt.setString(1, sqlStreamId.getId());
            cstmt.setInt(2, streamVersion);
            cstmt.setInt(3, count == Integer.MAX_VALUE ? count : count + 1);
            cstmt.registerOutParameter(4, Types.INTEGER);
            cstmt.registerOutParameter(5, Types.BIGINT);
            cstmt.registerOutParameter(6, Types.REF_CURSOR);

            cstmt.execute();

            int lastStreamVersion = cstmt.getInt(4);
            // streamNotFound page
            if (cstmt.wasNull()) {
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
            long lastStreamPosition = cstmt.getLong(5);

            int resultSetCount = 0;
            List<StreamMessage> messages = new ArrayList<>();
            try (ResultSet result = (ResultSet) cstmt.getObject(6)) {

                while (result.next()) {
                    if (messages.size() < count) {
                        int streamVersion1 = result.getInt(1);
                        long ordinal = result.getLong(2);
                        UUID messageId = (UUID) result.getObject(3);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = result.getTimestamp(5).toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
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

    @Override
    protected StreamSubscription subscribeToStreamInternal(String streamId,
                                                           Integer startVersion,
                                                           StreamMessageReceived streamMessageReceived,
                                                           SubscriptionDropped subscriptionDropped,
                                                           HasCaughtUp hasCaughtUp,
                                                           boolean prefetchJsonData,
                                                           String name) {

        return new StreamSubscriptionImpl(
            streamId,
            startVersion,
            this,
            getStoreObservable(),
            streamMessageReceived,
            subscriptionDropped,
            hasCaughtUp,
            prefetchJsonData,
            name);
    }

    @Override
    protected AllStreamSubscription subscribeToAllInternal(Long fromPosition,
                                                           AllStreamMessageReceived streamMessageReceived,
                                                           AllSubscriptionDropped subscriptionDropped,
                                                           HasCaughtUp hasCaughtUp,
                                                           boolean prefetchJsonData,
                                                           String name) {
        return new AllStreamSubscriptionImpl(
            fromPosition,
            this,
            getStoreObservable(),
            streamMessageReceived,
            subscriptionDropped,
            hasCaughtUp,
            prefetchJsonData,
            name);
    }


    //private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;
    // TODO: I think we can just return StreamStoreNotifier here
    private ObservableSource getStoreObservable() {
        return streamStoreNotifier.get();
    }

}

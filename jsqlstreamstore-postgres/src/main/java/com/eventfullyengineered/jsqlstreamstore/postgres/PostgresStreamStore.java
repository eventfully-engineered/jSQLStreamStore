package com.eventfullyengineered.jsqlstreamstore.postgres;

import com.eventfullyengineered.jsqlstreamstore.common.ResultSets;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.Empty;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.serialization.JsonSerializerStrategy;
import com.eventfullyengineered.jsqlstreamstore.store.ConnectionFactory;
import com.eventfullyengineered.jsqlstreamstore.store.StreamStoreBase;
import com.eventfullyengineered.jsqlstreamstore.streams.*;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.*;
import com.fasterxml.uuid.Generators;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.reactivex.ObservableSource;
import org.apache.commons.text.StringEscapeUtils;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Types;
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
    public ReadAllPage readAllForwardsInternal(long fromPositionExclusive, long maxCount, boolean prefetch, ReadNextAllPage readNextAllPage) throws SQLException {
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
                        long streamVersion = result.getLong(3);
                        ordinal = result.getLong(4);
                        UUID messageId = (UUID) result.getObject(5);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = ResultSets.toLocalDateTime(result.getTimestamp(6));
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
    public ReadAllPage readAllBackwardsInternal(long fromPositionExclusive,
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
                        long streamVersion = result.getLong(3);
                        ordinal = result.getLong(4);
                        UUID messageId = (UUID) result.getObject(5);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = ResultSets.toLocalDateTime(result.getTimestamp(6));
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
    public ReadStreamPage readStreamForwardsInternal(String streamName,
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
    public ReadStreamPage readStreamBackwardsInternal(String streamName,
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
    public StreamMetadataResult getStreamMetadataInternal(String streamName) throws SQLException {
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
    protected AppendResult appendToStreamInternal(
        String streamName,
        long expectedVersion,
        NewStreamMessage[] messages) throws SQLException {

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

    private AppendResult appendToStreamExpectedVersionAny(Connection connection,
                                                          String streamName,
                                                          NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (CallableStatement cstmt = connection.prepareCall(scripts.getAppendStreamExpectedVersionAny())) {
            connection.setAutoCommit(false);

            cstmt.setString(1, streamName);

            // create new message type array
            PGobject[] objects = convertToPGObjectArray(messages);
            Array array = connection.createArrayOf("new_message", objects);
            cstmt.setObject(2, array, Types.ARRAY);

            cstmt.execute();
            connection.commit();

            try (ResultSet rs = cstmt.getResultSet()) {
                rs.next();

                MetadataMessage streamMetadataMessage = rs.getString(1) == null
                    ? null
                    : jsonSerializerStrategy.fromJson(rs.getString(1), MetadataMessage.class);

                Long maxCount = streamMetadataMessage == null ? null : streamMetadataMessage.getMaxCount();

                return new AppendResult(maxCount, rs.getInt(2), rs.getInt(3));
            }

        } catch (SQLException ex) {
            connection.rollback();
            // Should we catch a PGSqlException instead?
            // https://github.com/SQLStreamStore/SQLStreamStore/blob/f7c3045f74d14be120f0c15cb0b25c48c173f012/src/SqlStreamStore.MsSql/MsSqlStreamStore.AppendStream.cs#L177
            // SQLIntegrityConstraintViolationException
            // TODO: fix thix. should be contains but SqlCode might be better????
            if ("IX_Messages_StreamIdInternal_Id".equals(ex.getMessage())) {
                long streamVersion = getStreamVersionOfMessageId(
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
                    throw new WrongExpectedVersion(streamName, ExpectedVersion.ANY, ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(streamName, ExpectedVersion.ANY, ex);
                    }
                }
                return new AppendResult(
                    null,
                    page.getLastStreamVersion(),
                    page.getLastStreamPosition());
            }

            // TODO: fix this....doesn't seem to work. check docs
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                throw new WrongExpectedVersion(streamName, ExpectedVersion.ANY, ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private long getStreamVersionOfMessageId(Connection connection,
                                            String streamName,
                                            UUID messageId) throws SQLException {

        try (CallableStatement command = connection.prepareCall(scripts.getStreamVersionOfMessageId())) {
            connection.setAutoCommit(false);

            command.setString(1, streamName);
            command.setString(2, messageId.toString());
            command.execute();

            try (ResultSet rs = command.getResultSet()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private AppendResult appendToStreamExpectedVersionNoStream(Connection connection,
                                                               String streamName,
                                                               NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (CallableStatement cstmt = connection.prepareCall(scripts.getAppendStreamExpectedVersionNoStream())) {
            connection.setAutoCommit(false);

            cstmt.setString(1, streamName);

            // create new message type array
            PGobject[] objects = convertToPGObjectArray(messages);
            Array array = connection.createArrayOf("new_message", objects);
            cstmt.setObject(2, array, Types.ARRAY);

            boolean executeResult = cstmt.execute();
            connection.commit();

            try (ResultSet rs = cstmt.getResultSet()) {
                rs.next();
                return new AppendResult(ResultSets.getLong(rs, 1), rs.getInt(2), rs.getInt(3));
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
                LOG.warn("integrity constraint violation");
            }

            if ("23505".equals(ex.getSQLState())) {
                ReadStreamPage page = readStreamInternal(
                    // sqlStreamId,
                    streamName,
                    StreamVersion.START,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM, ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM, ex);
                    }
                }
                return new AppendResult(
                    null,
                    page.getLastStreamVersion(),
                    page.getLastStreamPosition());
            }

            // TODO: fix this....doesn't seem to work. check docs
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                throw new WrongExpectedVersion(streamName, ExpectedVersion.NO_STREAM, ex);
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
            pgObject.setType("new_message");

            // TODO: find way to avoid having to pass date

            System.out.println(StringEscapeUtils.escapeJson(message.getData()));

            // TODO: fix this...it sucks
            // json data contains commas so we need to wrap value in quotes so postgres doesnt intrepret
            // comma as an argument separator

            Joiner j = Joiner.on(",").useForNull("");
            String s = j.join(
                message.getMessageId().toString(),
                message.getType(),
                String.valueOf(i + 1),
                escapeJson(message.getData()),
                escapeJson(message.getMetadata()),
                date.toString());
            pgObject.setValue("(" +  s + ")");
            objects[i] = pgObject;
            i++;
        }
        return objects;
    }

    private static String escapeJson(String json) {
        if (json == null) {
            return null;
        }

        return "\"" + StringEscapeUtils.escapeJson(json) + "\"";
    }


    private AppendResult appendToStreamExpectedVersion(Connection connection,
                                                       String streamName,
                                                       long expectedVersion,
                                                       NewStreamMessage[] messages) throws SQLException {

        // TODO: is this how we want to handle this?
        if (messages == null || messages.length == 0) {
            throw new IllegalArgumentException("messages must not be null or empty");
        }

        try (CallableStatement cstmt = connection.prepareCall(scripts.getAppendStreamExpectedVersion())) {
            connection.setAutoCommit(false);
            cstmt.setString(1, streamName);
            cstmt.setLong(2, expectedVersion);

            // create new message type array
            PGobject[] objects = convertToPGObjectArray(messages);
            Array array = connection.createArrayOf("new_message", objects);
            cstmt.setObject(3, array, Types.ARRAY);

            cstmt.execute();
            connection.commit();

            try (ResultSet rs = cstmt.getResultSet()) {
                rs.next();
                return new AppendResult(ResultSets.getLong(rs, 1), rs.getInt(2), rs.getInt(3));
            }

        } catch (SQLException ex) {
            connection.rollback();
            // Should we catch a PGSqlException instead?
            // https://github.com/SQLStreamStore/SQLStreamStore/blob/f7c3045f74d14be120f0c15cb0b25c48c173f012/src/SqlStreamStore.MsSql/MsSqlStreamStore.AppendStream.cs#L372
            if ("WrongExpectedVersion".equals(ex.getMessage())) {
                ReadStreamPage page = readStreamInternal(
                    // sqlStreamId,
                    streamName,
                    expectedVersion + 1,
                    messages.length,
                    ReadDirection.FORWARD,
                    false,
                    null,
                    connection);

                if (messages.length > page.getMessages().length) {
                    throw new WrongExpectedVersion(streamName, expectedVersion, ex);
                }

                for (int i = 0; i < Math.min(messages.length, page.getMessages().length); i++) {
                    if (!Objects.equals(messages[i].getMessageId(), page.getMessages()[i].getMessageId())) {
                        throw new WrongExpectedVersion(streamName, expectedVersion, ex);
                    }
                }
                return new AppendResult(
                    null,
                    page.getLastStreamVersion(),
                    page.getLastStreamPosition());
            }

            // TODO: fix this...this
            if (ex instanceof SQLIntegrityConstraintViolationException) {
                throw new WrongExpectedVersion(streamName, expectedVersion, ex);
            }

            // throw ex;
            throw new RuntimeException("sql exception occurred", ex);
        }

    }

    private void deleteStreamAnyVersion(String streamName) throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);
            deleteStreamAnyVersion(connection, streamName);
            // Delete metadata stream (if it exists)
            deleteStreamAnyVersion(connection, streamName);
            connection.commit();
        }
    }

    private void deleteStreamAnyVersion(Connection connection, String streamName) throws SQLException {

        boolean aStreamIsDeleted;
        try (CallableStatement stmt = connection.prepareCall(scripts.deleteStreamAnyVersion())) {
            stmt.setString(1, streamName);
            int count = stmt.executeUpdate();

            aStreamIsDeleted = count > 0;
        }

        if (aStreamIsDeleted) {
            NewStreamMessage streamDeletedEvent = Deleted.createStreamDeletedMessage(streamName);
            appendToStreamExpectedVersionAny(connection, Deleted.DELETED_STREAM_ID, new NewStreamMessage[] { streamDeletedEvent });
        }

    }

    @Override
    protected void deleteMessageInternal(String streamName, UUID messageId) throws SQLException {

        // TODO: could we batch the delete and inserts?
        // TODO: SQLStreamStore also does this via a taskqueue
        try (Connection connection = connectionFactory.openConnection()) {
            connection.setAutoCommit(false);

            try (CallableStatement stmt = connection.prepareCall(scripts.deleteStreamMessage())) {
                stmt.setString(1, streamName);
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

    @Override
    protected SetStreamMetadataResult setStreamMetadataInternal(String streamName,
                                                                long expectedStreamMetadataVersion,
                                                                Integer maxAge,
                                                                Long maxCount,
                                                                String metadataJson) throws SQLException {
        AppendResult result;
        try (Connection connection = connectionFactory.openConnection()) {
            MetadataMessage metadataMessage = new MetadataMessage(
                streamName,
                maxAge,
                maxCount,
                metadataJson);
            String json = metadataMessage.toString();
            NewStreamMessage newMessage = new NewStreamMessage(Generators.timeBasedGenerator().generate(), "$stream-metadata", json);

            result = appendToStreamInternal(
                connection,
                // sqlStreamId.getMetadataSqlStreamId(),
                streamName,
                expectedStreamMetadataVersion,
                new NewStreamMessage[]{newMessage});

            // TODO: reactive/task
            checkStreamMaxCount(streamName, maxCount);

            return new SetStreamMetadataResult(result.getCurrentVersion());
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

        try (CallableStatement cstmt = connection.prepareCall(commandText)) {
            connection.setAutoCommit(false);

            cstmt.setString(1, streamName);
            cstmt.setLong(2, startVersion);
            cstmt.setLong(3, count == Long.MAX_VALUE ? count : count + 1);
            cstmt.registerOutParameter(4, Types.BIGINT);
            cstmt.registerOutParameter(5, Types.BIGINT);
            cstmt.registerOutParameter(6, Types.REF_CURSOR);

            cstmt.execute();

            long lastStreamVersion = cstmt.getLong(4);
            // streamNotFound page
            if (cstmt.wasNull()) {
                return new ReadStreamPage(
                    streamName,
                    // sqlStreamId.getOriginalId(),
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
                        int streamId = result.getInt(1);
                        long version = result.getLong(2);
                        long position = result.getLong(3);
                        UUID messageId = (UUID) result.getObject(4);
                        // TODO: I dont think we need or want this
                        LocalDateTime created = ResultSets.toLocalDateTime(result.getTimestamp(5));
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

            return new ReadStreamPage(
                streamName,
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

    // TODO: make async/reactive
    private void checkStreamMaxCount(String streamName, Long maxCount) throws SQLException {
        if (maxCount != null) {
            Integer count = getStreamMessageCount(streamName);
            if (count > maxCount) {
                long toPurge = count - maxCount;
                ReadStreamPage streamMessagesPage = readStreamForwardsInternal(
                    streamName,
                    StreamVersion.START,
                    toPurge,
                    false,
                    null);

                if (streamMessagesPage.getStatus() == PageReadStatus.SUCCESS) {
                    for (StreamMessage message : streamMessagesPage.getMessages()) {
                        deleteMessageInternal(streamName, message.getMessageId());
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

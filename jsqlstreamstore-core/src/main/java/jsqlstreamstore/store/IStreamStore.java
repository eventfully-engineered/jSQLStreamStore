package jsqlstreamstore.store;

import jsqlstreamstore.streams.AppendResult;
import jsqlstreamstore.streams.NewStreamMessage;
import jsqlstreamstore.streams.SetStreamMetadataResult;

import java.sql.SQLException;
import java.util.UUID;

public interface IStreamStore {

    /**
     *
     * @param streamId
     * @param expectedVersion
     * @param message
     * @return
     * @throws SQLException
     */
    AppendResult appendToStream(
        String streamId,
        int expectedVersion,
        NewStreamMessage message) throws SQLException;

    /**
     *
     * @param streamId
     * @param expectedVersion
     * @param messages
     * @return
     * @throws SQLException
     */
    AppendResult appendToStream(
        String streamId,
        int expectedVersion,
        NewStreamMessage[] messages) throws SQLException;


	/**
	 * Appends a collection of messages to a stream.
	 *
	 * <p>
	 * Idempotency and concurrency handling is dependent on the choice of expected version and the messages
     * to append.
     * 1. When expectedVersion = ExpectedVersion.NoStream and the stream already exists and the collection of
     *    message IDs are not already in the stream, then <see cref="WrongExpectedVersionException"/> is
     *    throw.
     * 2. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs don't exist in the
     *    stream, then they are appended
     * 3. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs exist in the stream,
     *    then idempotency is applied and nothing happens.
     * 4. When expectedVersion = ExpectedVersion.Any and of the collection of messages Ids some exist in the
     *    stream and some don't then a <see cref="WrongExpectedVersionException"/> will be throwm.
     * 5. When expectedVersion is specified and the stream current version does not match the
     *    collection of message IDs are are checked against the stream in the correct position then the
     *    operation is considered idempotent. Otherwise a <see cref="WrongExpectedVersionException"/> will be
     *    throwm.
	 *
	 * @param streamId The Stream Id of the stream to append the messages. Must not start with a '$'.
	 * @param expectedVersion The version of the stream that is expected. This is used to control concurrency and idempotency concerns. See ExpectedVersion.
	 * @param messages The collection of messages to append.
	 */
	//void appendToStream(
    //        String streamId,
    //        int expectedVersion,
    //        EventRecord[] messages);

	/**
	 * Hard deletes a stream and all of its messages. Deleting a stream will result in a '$stream-deleted'
     * message being appended to the '$deleted' stream. See <see cref="Deleted.StreamDeleted"/> for the
     * message structure.
	 * @param streamId The stream Id to delete.
	 * @param expectedVersion The stream expected version. See ExpectedVersion for const values.
	 */
	void deleteStream(String streamId, int expectedVersion /*= ExpectedVersion.Any*/) throws SQLException;

	// delete event -- do we want to support?
	/**
	 * Hard deletes a message from the stream. Deleting a message will result in a '$message-deleted'
     * message being appended to the '$deleted' stream. See <see cref="Deleted.MessageDeleted"/> for the
     * message structure.
	 * @param streamId stream to delete from
	 * @param messageId The message to delete. If the message doesn't exist then nothing happens.
	 */
	void deleteMessage(String streamId, UUID messageId) throws SQLException;

	/**
	 * Sets the metadata for a stream.
	 * @param streamId The stream Id to whose metadata is to be set.
	 * @param expectedStreamMetadataVersion  The expected version number of the metadata stream to apply the metadata. Used for concurrency
     * handling. Default value is <see cref="ExpectedVersion.Any"/>. If specified and does not match
     * current version then <see cref="WrongExpectedVersionException"/> will be thrown.
     * @param maxAge The max age of the messages in the stream in seconds.
     * @param maxCount The max count of messages in the stream.
     * @param metadataJson Custom meta data to associate with the stream.
	 */
	SetStreamMetadataResult setStreamMetadata(
        String streamId,
        int expectedStreamMetadataVersion, // = ExpectedVersion.Any,
        Integer maxAge,
        Integer maxCount,
        String metadataJson) throws SQLException;
}

package jsqlstreamstore.store;

import com.google.common.base.Preconditions;
import jsqlstreamstore.streams.AppendResult;
import jsqlstreamstore.streams.NewStreamMessage;
import jsqlstreamstore.streams.SetStreamMetadataResult;
import jsqlstreamstore.streams.StreamMessage;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 *
 *
 */
public abstract class StreamStoreBase extends ReadOnlyStreamStoreBase implements IStreamStore {

    private final Queue<Consumer> purgeQueue = new ArrayBlockingQueue<>(100);
    private final ExecutorService purgeExecutor = Executors.newSingleThreadExecutor();

    protected final Logger logger = LoggerFactory.getLogger(StreamStoreBase.class);

    protected StreamStoreBase(
            Period metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize) {
        super(metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize);
    }

    @Override
    public AppendResult appendToStream(String streamId, int expectedVersion, NewStreamMessage message) {
	    Preconditions.checkArgument(!streamId.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");
	    Preconditions.checkNotNull(message);

	    logger.debug("AppendToStream {} with expected version {}", streamId, expectedVersion);

	    return null;
    }

    @Override
    public AppendResult appendToStream(String streamId, int expectedVersion, NewStreamMessage[] messages) throws SQLException {
        Preconditions.checkArgument(!streamId.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");
        Preconditions.checkNotNull(messages);

        logger.debug("AppendToStream {} with expected version {} and {} messages.", streamId, expectedVersion, messages.length);
        if (messages.length == 0 && expectedVersion >= 0) {
            // If there is an expected version then nothing to do...
            // Expose CurrentPosition as part of AppendResult
            return createAppendResultAtHeadPosition(expectedVersion);
        }
        // ... expectedVersion.NoStream and ExpectedVersion.Any may create an empty stream though
        return appendToStreamInternal(streamId, expectedVersion, messages);
    }

    /**
     * https://github.com/SQLStreamStore/SQLStreamStore/pull/70
     *
     * In cases where you want to read your own writes you may want to echo the last position you've just written.
     * This PR exposes this last written position by an append operation as CurrentPosition on the AppendResult.
     * Consumers can now pass on this CurrentPosition to clients (if applicable) and wait for non stale results up until at least that CurrentPosition.
     * If one's using HTTP, one could echo this position as part of the response (in the header or payload).
     * If you're using another transport medium, you could do something similar.
     * Now, that same position is often used to jsqlstreamstore.store how far a projection has caught up.
     * In such a case, waiting for non stale projections means you're waiting until one or more projections have reached at least the given position.
     * Again, combined with HTTP, the position could be an important ingredient in the ETags you build up and your overall resource caching strategy.
     *
     * Fixed: There's a conceptual problem I don't really know how to solve, which is, what is the CurrentPosition to be when you append with ExpectedVersion.NoStream and no messages - we could return the HEAD position.
     * As soon as there are events, both the in memory and the sql implementation seem to be doing the right thing.
     *
     */
    private AppendResult createAppendResultAtHeadPosition(int expectedVersion) {
        Long position = readHeadPosition();
        return new AppendResult(expectedVersion, position);
    }

    @Override
	public void deleteStream(String streamId, int expectedVersion) throws SQLException {
        Preconditions.checkArgument(!streamId.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");

        logger.debug("DeleteStream {} with expected version {}.", streamId, expectedVersion);

        deleteStreamInternal(streamId, expectedVersion);
	}

	@Override
	public void deleteMessage(String streamId, UUID messageId) throws SQLException {
        Preconditions.checkArgument(!streamId.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");

	    logger.debug("DeleteMessage {} with messageId {}", streamId, messageId);

        deleteMessageInternal(streamId, messageId);

	}

	@Override
	public SetStreamMetadataResult setStreamMetadata(
	        String streamId,
	        int expectedStreamMetadataVersion,
	        Integer maxAge,
	        Integer maxCount,
			String metadataJson) throws SQLException {
        Preconditions.checkArgument(!streamId.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");
        Preconditions.checkArgument(expectedStreamMetadataVersion >= -2, "expectedStreamMetadataVersion must be greater than or equal to -2");

        logger.debug("SetStreamMetadata {} with expected metadata version {}, max age {} and max count {}.",
                streamId, expectedStreamMetadataVersion, maxAge, maxCount);

        return setStreamMetadataInternal(
            streamId,
            expectedStreamMetadataVersion,
            maxAge,
            maxCount,
            metadataJson);
	}

	/**
	 * Gets the count of messages in a stream
	 * @param streamId The stream id
	 * @return The number of messages in a stream.
	 */
    protected abstract int getStreamMessageCount(String streamId) throws SQLException;


//    /// <summary>
//    ///     Queues a task to purge expired message.
//    /// </summary>
//    /// <param name="streamMessage"></param>
//    protected override void PurgeExpiredMessage(StreamMessage streamMessage)
//    {
//        _taskQueue.Enqueue(ct => DeleteEventInternal(streamMessage.StreamId, streamMessage.MessageId, ct));
//    }


    /**
     * Queues a task to purge expired message.
     * @param streamMessage
     */
    @Override
    protected void purgeExpiredMessage(StreamMessage streamMessage) throws SQLException {
//        purgeQueue.add(new Consumer<StreamMessage>() {
//            @Override
//            public void accept(StreamMessage messsage) {
//                //deleteMessageInternal(streamMessage.getStreamId(), streamMessage.getMessageId());
//            }
//        });
        // TODO: this needs to queue a task. Executorservice
        deleteMessageInternal(streamMessage.getStreamId(), streamMessage.getMessageId());
    }

    protected abstract AppendResult appendToStreamInternal(String streamId, int expectedVersion,
            NewStreamMessage[] messages) throws SQLException;

    protected abstract void deleteStreamInternal(String streamId, int expectedVersion) throws SQLException;

    protected abstract void deleteMessageInternal(String streamId, UUID messageId) throws SQLException;

    protected abstract SetStreamMetadataResult setStreamMetadataInternal(
            String streamId,
            int expectedStreamMetadataVersion,
            Integer maxAge,
            Integer maxCount,
            String metadataJson) throws SQLException;

}

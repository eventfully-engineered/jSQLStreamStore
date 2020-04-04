package jsqlstreamstore.store;

import com.google.common.base.Preconditions;
import jsqlstreamstore.streams.AppendResult;
import jsqlstreamstore.streams.NewStreamMessage;
import jsqlstreamstore.streams.SetStreamMetadataResult;
import jsqlstreamstore.streams.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 *
 *
 */
public abstract class StreamStoreBase extends ReadOnlyStreamStoreBase implements IStreamStore {

    protected final Logger logger = LoggerFactory.getLogger(StreamStoreBase.class);

    // SynchronousQueue
    private final Queue<Consumer> purgeQueue = new ArrayBlockingQueue<>(100);
    // TODO: allow executor to be passed in
    private final ExecutorService purgeExecutor = Executors.newSingleThreadExecutor();

    ExecutorService exService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());

    protected StreamStoreBase(Duration metadataMaxAgeCacheExpiry,
                              int metadataMaxAgeCacheMaxSize) {
        super(metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize);
    }

    @Override
    public AppendResult appendToStream(String streamName, long expectedVersion, NewStreamMessage message) throws SQLException {
	    Preconditions.checkArgument(!streamName.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");
	    Preconditions.checkNotNull(message);

	    logger.debug("AppendToStream {} with expected version {}", streamName, expectedVersion);

	    return appendToStream(streamName, expectedVersion, new NewStreamMessage[] { message });
    }

    @Override
    public AppendResult appendToStream(String streamName, long expectedVersion, NewStreamMessage[] messages) throws SQLException {
        Preconditions.checkArgument(!streamName.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");
        Preconditions.checkNotNull(messages);

        logger.debug("AppendToStream {} with expected version {} and {} messages.", streamName, expectedVersion, messages.length);
        if (messages.length == 0 && expectedVersion >= 0) {
            // If there is an expected version then nothing to do...
            // Expose CurrentPosition as part of AppendResult
            return createAppendResultAtHeadPosition(expectedVersion);
        }
        // ... expectedVersion.NoStream and ExpectedVersion.Any may create an empty stream though
        return appendToStreamInternal(streamName, expectedVersion, messages);
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
    private AppendResult createAppendResultAtHeadPosition(long expectedVersion) {
        Long position = readHeadPosition();
        return new AppendResult(expectedVersion, position);
    }

    @Override
	public void deleteStream(String streamName, long expectedVersion) throws SQLException {
        Preconditions.checkArgument(!streamName.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");

        logger.debug("DeleteStream {} with expected version {}.", streamName, expectedVersion);

        deleteStreamInternal(streamName, expectedVersion);
	}

	@Override
	public void deleteMessage(String streamName, UUID messageId) throws SQLException {
        Preconditions.checkArgument(!streamName.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");

	    logger.debug("DeleteMessage {} with messageId {}", streamName, messageId);

        deleteMessageInternal(streamName, messageId);

	}

	@Override
	public SetStreamMetadataResult setStreamMetadata(
	        String streamName,
	        long expectedStreamMetadataVersion,
	        Integer maxAge,
	        Long maxCount,
			String metadataJson) throws SQLException {
        Preconditions.checkArgument(!streamName.startsWith("$"), "streamId must not start with $ as this is dedicated for internal system streams");
        Preconditions.checkArgument(expectedStreamMetadataVersion >= -2, "expectedStreamMetadataVersion must be greater than or equal to -2");

        logger.debug("SetStreamMetadata {} with expected metadata version {}, max age {} and max count {}.",
            streamName, expectedStreamMetadataVersion, maxAge, maxCount);

        return setStreamMetadataInternal(
            streamName,
            expectedStreamMetadataVersion,
            maxAge,
            maxCount,
            metadataJson);
	}

	/**
	 * Gets the count of messages in a stream
	 * @param streamName The stream name
	 * @return The number of messages in a stream.
	 */
    protected abstract int getStreamMessageCount(String streamName) throws SQLException;

    /**
     * Queues a task to purge expired message.
     * @param streamMessage The message to purge
     */
    @Override
    protected void purgeExpiredMessage(StreamMessage streamMessage) throws SQLException {
        purgeExecutor.execute(() -> {
            try {
                deleteMessageInternal(streamMessage.getStreamName(), streamMessage.getMessageId());
            } catch (SQLException e) {
                logger.error("failed to purge expired message {}", streamMessage, e);
            }
        });
    }

    protected abstract AppendResult appendToStreamInternal(String streamName,
                                                           long expectedVersion,
                                                           NewStreamMessage[] messages) throws SQLException;

    protected abstract void deleteStreamInternal(String streamName, long expectedVersion) throws SQLException;

    protected abstract void deleteMessageInternal(String streamName, UUID messageId) throws SQLException;

    protected abstract SetStreamMetadataResult setStreamMetadataInternal(String streamName,
                                                                         long expectedStreamMetadataVersion,
                                                                         Integer maxAge,
                                                                         Long maxCount,
                                                                         String metadataJson) throws SQLException;

}

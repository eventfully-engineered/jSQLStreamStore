package jsqlstreamstore.store;

import jsqlstreamstore.streams.ReadAllPage;
import jsqlstreamstore.streams.ReadStreamPage;
import jsqlstreamstore.subscriptions.StreamMessageReceived;
import jsqlstreamstore.streams.StreamMetadataResult;
import jsqlstreamstore.subscriptions.*;

import java.sql.SQLException;

/**
 * Represents a readonly stream store.
 *
 */
// TODO: does this need to implement closeable or Disposable so that we can clean; specifically subscriptions?
// or perhaps something like OnDispose
public interface IReadOnlyStreamStore {


    /**
     *
     * @param fromPositionInclusive position to start reading from. Use Position.START to start from the beginning
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadAllPage readAllForwards(long fromPositionInclusive, long maxCount, boolean prefetch) throws SQLException;

    /**
     *
     * @param fromPositionInclusive The position to start reading from. Use Position.END to start from the end.
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadAllPage readAllBackwards(long fromPositionInclusive, long maxCount, boolean prefetch) throws SQLException;

    /**
     *
     * @param streamName the stream to read
     * @param fromVersionInclusive The version of the stream to start reading from. Use StreamVersion.Start to read from the start.
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadStreamPage readStreamForwards(
        String streamName,
        long fromVersionInclusive,
        long maxCount,
        boolean prefetch) throws SQLException;

    /**
     *
     * @param streamName the stream to read
     * @param fromVersionInclusive The version of the stream to start reading from. Use StreamVersion.End to read from the end
     * @param maxCount maximum number of events to read
     * @param prefetch Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @return An @{link ReadAllPage} presenting the result of the read. If all messages read have expired then the message collection MAY be empty.
     */
    ReadStreamPage readStreamBackwards(String streamName,
                                       long fromVersionInclusive,
                                       long maxCount,
                                       boolean prefetch) throws SQLException;


	/**
	 * Reads the head position (the position of the very latest message).
	 * @return the head position
	 */
	Long readHeadPosition();

//	/**
//	 * Gets the stream metadata
//	 * @param streamId The stream ID whose metadata is to be read.
//	 */
//	StreamMetadataResult getStreamMetadata(String streamId) throws SQLException;

    /**
     *
     * @param streamName  stream to subscribe to
     * @param continueAfterVersion fromVersionExclusive the version to describe from
     * @param streamMessageReceived streamMessageReceived a delegate that is invoked when a message is available. If an exception is thrown the subscription is terminated
     * @param subscriptionDropped subscriptionDropped a delegate that is invoked when a subscription fails
     * @param hasCaughtUp A delegate that is invoked with value=true when the subscription has caught up with the stream (when the underlying page read has IsEnd=true) and when it falls behind (when the underlying page read has IsEnd=false).
     * @param prefetchJsonData Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @param name name the name of the subscription used for logging. (optional)
     * @return
     */
    StreamSubscription subscribeToStream(
        String streamName,
        Integer continueAfterVersion,
        StreamMessageReceived streamMessageReceived,
        SubscriptionDropped subscriptionDropped,
        HasCaughtUp hasCaughtUp,
        boolean prefetchJsonData,
        String name);

    /**
     *
     * @param continueAfterPosition The position to start subscribing after. Use null to include the first message.
     * @param streamMessageReceived A delegate that is invoked when a message is available. If an exception is thrown, the subscription is terminated.
     * @param subscriptionDropped A delegate that is invoked when a the subscription is dropped. This will be invoked once and only once.
     * @param hasCaughtUp A delegate that is invoked with value=true when the subscription has catught up with the all stream (when the underlying page read has IsEnd=true) and when it falls behind (when the underlying page read has IsEnd=false).
     * @param prefetchJsonData Prefetches the message data as part of the page read. This means a single request to the server but a higher payload size.
     * @param name The name of the subscription used for logging. Optional.
     * @return
     */
    AllStreamSubscription subscribeToAll(
        Long continueAfterPosition,
        AllStreamMessageReceived streamMessageReceived,
        AllSubscriptionDropped subscriptionDropped,
        HasCaughtUp hasCaughtUp,
        boolean prefetchJsonData,
        String name);
}

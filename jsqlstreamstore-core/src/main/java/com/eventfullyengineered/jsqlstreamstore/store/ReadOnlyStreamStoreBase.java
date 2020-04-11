package com.eventfullyengineered.jsqlstreamstore.store;

import com.eventfullyengineered.jsqlstreamstore.infrastructure.Ensure;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.MetadataMaxAgeCache;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadAllPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadNextAllPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadNextStreamPage;
import com.eventfullyengineered.jsqlstreamstore.streams.ReadStreamPage;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamMessage;
import com.eventfullyengineered.jsqlstreamstore.streams.StreamMetadataResult;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.AllStreamMessageReceived;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.AllStreamSubscription;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.AllSubscriptionDropped;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.HasCaughtUp;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.StreamMessageReceived;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.StreamSubscription;
import com.eventfullyengineered.jsqlstreamstore.subscriptions.SubscriptionDropped;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public abstract class ReadOnlyStreamStoreBase implements IReadOnlyStreamStore {

    private final Logger LOG = LoggerFactory.getLogger(ReadOnlyStreamStoreBase.class);

    private static final int DEFAULT_RELOAD_INTERVAL = 3000;

    private final MetadataMaxAgeCache _metadataMaxAgeCache;

    // TODO: change to java time Period?
    protected ReadOnlyStreamStoreBase(
        Duration metadataMaxAgeCacheExpiry,
        int metadataMaxAgeCacheMaxSize) {
        _metadataMaxAgeCache = new MetadataMaxAgeCache(this, metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize);
    }

	@Override
	public ReadAllPage readAllForwards(final long fromPositionInclusive, final long maxCount, final boolean prefetch) throws SQLException {
	    Ensure.nonnegative(fromPositionInclusive, "fromPositionInclusive");
	    Ensure.positive(maxCount, "maxCount");

        LOG.debug("ReadAllForwards from position {} with max count {}.", fromPositionInclusive, maxCount);

        ReadNextAllPage readNext = (Long nextPosition) -> readAllForwards(nextPosition, maxCount, prefetch);

	    ReadAllPage page = readAllForwardsInternal(fromPositionInclusive, maxCount, prefetch, readNext);

        // https://github.com/damianh/SqlStreamStore/issues/31
        // Under heavy parallel load, gaps may appear in the position sequence due to sequence
        // number reservation of in-flight transactions.
        // Here we check if there are any gaps, and in the unlikely event there is, we delay a little bit
        // and re-issue the read. This is expected
        if (!page.isEnd() || page.getMessages().length <= 1) {
            return filterExpired(page, readNext);
        }

        // Check for gap between last page and this.
        if (page.getMessages()[0].getPosition() != fromPositionInclusive) {
            page = reloadAfterDelay(fromPositionInclusive, maxCount, prefetch, readNext);
        }

        // check for gap in messages collection
        for (int i = 0; i < page.getMessages().length - 1; i++) {
            if (page.getMessages()[i].getPosition() + 1 != page.getMessages()[i + 1].getPosition()) {
                page = reloadAfterDelay(fromPositionInclusive, maxCount, prefetch, readNext);
                break;
            }
        }

        return filterExpired(page, readNext);

	}

	@Override
	public ReadAllPage readAllBackwards(long fromPositionInclusive, long maxCount, boolean prefetch)  throws SQLException {
	    Preconditions.checkArgument(fromPositionInclusive >= -1);
	    Ensure.positive(maxCount, "maxCount");

        LOG.debug("ReadAllBackwards from position {} with max count {}.", fromPositionInclusive, maxCount);

		ReadNextAllPage readNext = (Long nextPosition) -> readAllBackwards(nextPosition, maxCount, prefetch);

	    ReadAllPage page = readAllBackwardsInternal(fromPositionInclusive, maxCount, prefetch, readNext);

        return filterExpired(page, readNext);
	}

	@Override
	public ReadStreamPage readStreamForwards(String streamName, long fromVersionInclusive, long maxCount, boolean prefetch) throws SQLException {
		// TODO: do we need -- ensure stream id is not null, empty or whitespaces
        Preconditions.checkArgument(fromVersionInclusive >= 0);
		Preconditions.checkArgument(maxCount >= 1);

        LOG.debug("ReadStreamForwards {} from version {} with max count {}.", streamName, fromVersionInclusive, maxCount);

        ReadNextStreamPage readNext = (long nextPosition) -> readStreamForwards(streamName, fromVersionInclusive, maxCount, prefetch);

        ReadStreamPage page = readStreamForwardsInternal(streamName, fromVersionInclusive, maxCount, prefetch, readNext);

        return filterExpired(page, readNext);
	}

	@Override
	public ReadStreamPage readStreamBackwards(String streamName, long fromVersionInclusive, long maxCount, boolean prefetch) throws SQLException {
        // TODO: do we need -- ensure stream id is not null, empty or whitespaces
        Preconditions.checkArgument(fromVersionInclusive >= -1);
        Preconditions.checkArgument(maxCount >= 1);

        LOG.debug("ReadStreamBackwards {} from version {} with max count {}.", streamName, fromVersionInclusive, maxCount);

        ReadNextStreamPage readNext = (long nextPosition) -> readStreamForwards(streamName, fromVersionInclusive, maxCount, prefetch);

		ReadStreamPage page = readStreamBackwardsInternal(streamName, fromVersionInclusive, maxCount, prefetch, readNext);

        return filterExpired(page, readNext);
	}

	@Override
	public Long readHeadPosition() {
		return readHeadPositionInternal();
	}

	@Override
	public StreamMetadataResult getStreamMetadata(String streamName) throws SQLException {
        Preconditions.checkArgument(!streamName.startsWith("$"), "streamName must not start with $ as this is dedicated for internal system streams");
	    return getStreamMetadataInternal(streamName);
	}

	@Override
    public StreamSubscription subscribeToStream(String streamName, Integer continueAfterVersion,
                                                StreamMessageReceived streamMessageReceived, SubscriptionDropped subscriptionDropped,
                                                HasCaughtUp hasCaughtUp, boolean prefetchJsonData, String name) {
        return subscribeToStreamInternal(
                streamName,
                continueAfterVersion,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
    }

    @Override
    public AllStreamSubscription subscribeToAll(Long continueAfterPosition,
                                                AllStreamMessageReceived streamMessageReceived, AllSubscriptionDropped subscriptionDropped,
                                                HasCaughtUp hasCaughtUp, boolean prefetchJsonData, String name) {
        return subscribeToAllInternal(
                continueAfterPosition,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
    }

    protected abstract StreamSubscription subscribeToStreamInternal(
            String streamName,
            Integer startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            boolean prefetchJsonData,
            String name);

    protected abstract AllStreamSubscription subscribeToAllInternal(
        Long fromPosition,
        AllStreamMessageReceived streamMessageReceived,
        AllSubscriptionDropped subscriptionDropped,
        HasCaughtUp hasCaughtUp,
        boolean prefetchJsonData,
        String name);

	protected abstract ReadAllPage readAllForwardsInternal(long fromPositionExclusive, long maxCount, boolean prefetch, ReadNextAllPage readNextAllPage) throws SQLException;

	protected abstract ReadAllPage readAllBackwardsInternal(long fromPositionExclusive, long maxCount, boolean prefetch, ReadNextAllPage readNextAllPage) throws SQLException;

	protected abstract ReadStreamPage readStreamForwardsInternal(String streamName, long start, long count, boolean prefetch, ReadNextStreamPage readNextStreamPage) throws SQLException;

	protected abstract ReadStreamPage readStreamBackwardsInternal(String streamName, long fromVersionInclusive, long count, boolean prefetch, ReadNextStreamPage readNextStreamPage) throws SQLException;

	protected abstract Long readHeadPositionInternal();

	protected abstract StreamMetadataResult getStreamMetadataInternal(String streamName) throws SQLException;

    protected abstract void purgeExpiredMessage(StreamMessage streamMessage) throws SQLException;

	// TODO: async task  / executor
    private ReadAllPage reloadAfterDelay(
        long fromPositionInclusive,
        long maxCount,
        boolean prefetch,
        ReadNextAllPage readNext) {

        try {
            LOG.info("ReadAllForwards: gap detected in position, reloading after {}ms", DEFAULT_RELOAD_INTERVAL);

            // this should just be something like task.delay
            Thread.sleep(DEFAULT_RELOAD_INTERVAL);

            ReadAllPage reloadedPage = readAllForwardsInternal(fromPositionInclusive, maxCount, prefetch, readNext);

            return filterExpired(reloadedPage, readNext);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // TODO: async task / executor
    private ReadStreamPage filterExpired(
        ReadStreamPage page,
        ReadNextStreamPage readNext) throws SQLException {

        if (page.getStreamName().startsWith("$")) {
            return page;
        }

        Integer maxAge = _metadataMaxAgeCache.getMaxAge(page.getStreamName());
        if (maxAge == null) {
            return page;
        }
        LocalDateTime currentUtc = LocalDateTime.now(ZoneId.of("UTC"));
        List<StreamMessage> valid = new ArrayList<>();
        for (StreamMessage message : page.getMessages()) {
            if (message.getCreatedUtc().plusSeconds(maxAge).isAfter(currentUtc)) {
                valid.add(message);
            } else {
                purgeExpiredMessage(message);
            }
        }
        return new ReadStreamPage(
            page.getStreamName(),
            page.getStatus(),
            page.getFromStreamVersion(),
            page.getNextStreamVersion(),
            page.getLastStreamVersion(),
            page.getLastStreamPosition(),
            page.getReadDirection(),
            page.isEnd(),
            readNext,
            valid.toArray(new StreamMessage[0]));
    }

    // TODO: async task / executor
    private ReadAllPage filterExpired(ReadAllPage readAllPage, ReadNextAllPage readNext) throws SQLException {

        List<StreamMessage> valid = new ArrayList<>();
        LocalDateTime currentUtc = LocalDateTime.now(ZoneId.of("UTC"));
        for (StreamMessage streamMessage : readAllPage.getMessages()) {
            if (streamMessage.getStreamName().startsWith("$")) {
                valid.add(streamMessage);
                continue;
            }
            Integer maxAge = _metadataMaxAgeCache.getMaxAge(streamMessage.getStreamName());
            if (maxAge == null) {
                valid.add(streamMessage);
                continue;
            }
            if (streamMessage.getCreatedUtc().plusSeconds(maxAge).isAfter(currentUtc)) {
                valid.add(streamMessage);
            } else {
                purgeExpiredMessage(streamMessage);
            }
        }
        return new ReadAllPage(
            readAllPage.getFromPosition(),
            readAllPage.getNextPosition(),
            readAllPage.isEnd(),
            readAllPage.getReadDirection(),
            readNext,
            valid.toArray(new StreamMessage[0]));
    }

}

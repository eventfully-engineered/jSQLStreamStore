package com.eventfullyengineered.jsqlstreamstore.infrastructure;

import com.eventfullyengineered.jsqlstreamstore.store.IReadOnlyStreamStore;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetadataMaxAgeCache {

    private final IReadOnlyStreamStore _store;
    private final Duration _expiration;
    private final int _maxSize;
    //private readonly GetUtcNow _getUtcNow;
    private final ConcurrentMap<String, MaxAgeCacheItem> _byStreamId = new ConcurrentHashMap<>();
    private final Queue<MaxAgeCacheItem> _byCacheStamp = new ConcurrentLinkedQueue<>(); // TODO: do we want to use ConcurrentLinkedQueue
    private AtomicLong _cacheHitCount = new AtomicLong(0);

    // GetUtcNow getUtcNow
    public MetadataMaxAgeCache(IReadOnlyStreamStore store, Duration expiration, int maxSize) {
        //Ensure.That(store, nameof(store)).IsNotNull();
        //Ensure.That(maxSize).IsGte(0);
        //Ensure.That(getUtcNow).IsNotNull();

        _store = store;
        _expiration = expiration;
        _maxSize = maxSize;
        //_getUtcNow = getUtcNow;
    }

    public long getCacheHitCount() {
        return  _cacheHitCount.get();
    }

    public int getCount() {
        return _byStreamId.size();
    }

    // async task
    public Integer getMaxAge(String streamId) throws SQLException {
        // TODO: use getUtcNow functional interface that would be passed in by callers/applications
        // var utcNow = _getUtcNow();
        LocalDateTime utcNow = LocalDateTime.now(ZoneId.of("UTC"));
        MaxAgeCacheItem cacheItem = _byStreamId.get(streamId);
        if (cacheItem != null) {
            LocalDateTime expiresAt = cacheItem.cachedStampUtc.plus(_expiration);
            if (expiresAt.isAfter(utcNow)) {
                _cacheHitCount.incrementAndGet();
                return cacheItem.maxAge;
            }
        }

//        StreamMetadataResult result = _store.getStreamMetadata(streamId);
//
//        cacheItem = new MaxAgeCacheItem(streamId, utcNow, result.getMaxAge());
//        _byStreamId.putIfAbsent(streamId, cacheItem);
//        _byCacheStamp.add(cacheItem);
//
//        while (_byCacheStamp.size() > _maxSize) {
//            cacheItem = _byCacheStamp.remove();
//            if (cacheItem != null) {
//                _byStreamId.remove(cacheItem.streamId);
//            }
//        }
//
//        return result.getMaxAge();
        return null;
    }


    private class MaxAgeCacheItem {
        final String streamId;
        final LocalDateTime cachedStampUtc;
        final Integer maxAge;

        MaxAgeCacheItem(String streamId, LocalDateTime cachedStampUtc, Integer maxAge) {
            this.streamId = streamId;
            this.cachedStampUtc = cachedStampUtc;
            this.maxAge = maxAge;
        }
    }
}

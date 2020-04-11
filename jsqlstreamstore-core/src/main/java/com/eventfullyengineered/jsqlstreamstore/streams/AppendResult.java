package com.eventfullyengineered.jsqlstreamstore.streams;

import com.google.common.base.MoreObjects;


/**
 * Represents the result returned after appending to a stream
 *
 */
public class AppendResult {

    private Long maxCount;

    /**
     * The current version of the stream after the append operation was performed
     *
     */
    private final long currentVersion;

    /**
     * The current position of the stream after the append operation was performed
     */
    private final long currentPosition;

    /**
     * Constructs new {@link AppendResult}
     * @param currentVersion The current version of the stream after the append operation was performed.
     * @param currentPosition The current position of the stream after the append operation was performed.
     */
    public AppendResult(long currentVersion, long currentPosition) {
        this(null, currentVersion, currentPosition);
    }

    /**
     * Constructs new {@link AppendResult}
     * @param currentVersion The current version of the stream after the append operation was performed.
     * @param currentPosition The current position of the stream after the append operation was performed.
     */
    public AppendResult(Long maxCount, long currentVersion, long currentPosition) {
        this.maxCount = maxCount;
        this.currentVersion = currentVersion;
        this.currentPosition = currentPosition;
    }

    public Long getMaxCount() {
        return maxCount;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public long getCurrentPosition() {
        return currentPosition;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("maxCount", getMaxCount())
                .add("currentVersion", getCurrentVersion())
                .add("currentPosition", getCurrentPosition())
                .toString();
    }
}

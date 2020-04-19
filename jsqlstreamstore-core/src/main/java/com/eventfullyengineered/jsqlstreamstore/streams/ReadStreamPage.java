package com.eventfullyengineered.jsqlstreamstore.streams;

import com.eventfullyengineered.jsqlstreamstore.infrastructure.Empty;
import com.google.common.base.MoreObjects;

import java.sql.SQLException;

/**
 * From SqlStreamStore
 * EventStore has StreamEventsSlice
 * Represents the result of a read from a stream.
 */
public class ReadStreamPage {

    /**
     * The collection of messages read.
     * EventStore has ResolvedEvent[] Events - The events read represented as <see cref="ResolvedEvent"/>
     */
    private final StreamMessage[] messages;

    private final ReadNextStreamPage readNext;

    /**
     * The version of the stream that read from.
     * EventStore has FromEventNumber - The starting point (represented as a sequence number) of the read operation.
     */
    private final long fromStreamVersion;

    /**
     * The position of the last message in the stream.
     * TODO: does EventStore have this?
     */
    private final long lastStreamPosition;

    /**
     * The next message version that can be read.
     * EventStore has NextEventNumber - The next event number that can be read.
     */
    private final long nextStreamVersion;

    /**
     * The version of the last message in the stream.
     * EventStore has LastEventNumber - The last event number in the stream.
     */
    private final long lastStreamVersion;

    /**
     * The direction of read operation.
     * Same in EventStore
     */
    private final ReadDirection readDirection;

    /**
     * The {@link PageReadStatus} of the read operation
     * EventStore has status property - The <see cref="SliceReadStatus"/> representing the status of this read attempt
     */
    private final PageReadStatus status;

    /**
     * The name of the stream that was read.
     * EventStore has stream property - The name of the stream read
     */
    private final String streamName;

    /**
     * Whether or not this is the end of the stream.
     * EventStore has IsEndOfStream - A boolean representing whether or not this is the end of the stream.
     */
    private final boolean isEnd;

    public ReadStreamPage(String streamName,
                          PageReadStatus status,
                          long fromStreamVersion,
                          long nextStreamVersion,
                          long lastStreamVersion,
                          long lastStreamPosition,
                          ReadDirection readDirection,
                          boolean isEnd,
                          ReadNextStreamPage readNext) {
        this(streamName, status, fromStreamVersion, nextStreamVersion, lastStreamVersion, lastStreamPosition, readDirection, isEnd, readNext, null);
    }

    /**
     *
     * @param streamName - The name of the stream that was read.
     * @param status - The <see cref="PageReadStatus"/> of the read operation.
     * @param fromStreamVersion - The version of the stream that read from.
     * @param nextStreamVersion - The next message version that can be read.
     * @param lastStreamVersion - The version of the last message in the stream.
     * @param lastStreamPosition - The position of the last message in the stream.
     * @param readDirection - The direction of the read operation.
     * @param isEnd - Whether or not this is the end of the stream.
     * @param readNext - The messages read.
     */
    public ReadStreamPage(String streamName,
                          PageReadStatus status,
                          long fromStreamVersion,
                          long nextStreamVersion,
                          long lastStreamVersion,
                          long lastStreamPosition,
                          ReadDirection readDirection,
                          boolean isEnd,
                          ReadNextStreamPage readNext,
                          StreamMessage[] messages) {
        this.streamName = streamName;
        this.status = status;
        this.fromStreamVersion = fromStreamVersion;
        this.lastStreamVersion = lastStreamVersion;
        this.lastStreamPosition = lastStreamPosition;
        this.nextStreamVersion = nextStreamVersion;
        this.readDirection = readDirection;
        this.isEnd = isEnd;
        this.messages = messages == null ? Empty.STREAM_MESSAGE : messages;
        this.readNext = readNext;
    }

    public StreamMessage[] getMessages() {
        return messages;
    }

    public long getFromStreamVersion() {
        return fromStreamVersion;
    }

    public long getLastStreamPosition() {
        return lastStreamPosition;
    }

    public long getNextStreamVersion() {
        return nextStreamVersion;
    }

    public long getLastStreamVersion() {
        return lastStreamVersion;
    }

    public ReadDirection getReadDirection() {
        return readDirection;
    }

    public PageReadStatus getStatus() {
        return status;
    }

    public String getStreamName() {
        return streamName;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public ReadStreamPage readNext() throws SQLException {
        return readNext.get(nextStreamVersion);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("fromStreamVersion", getFromStreamVersion())
                .add("lastStreamVersion", getLastStreamVersion())
                .add("status", getStatus())
                .add("streamName", getStreamName())
                .add("isEnd", isEnd())
                .add("readDirection", getReadDirection())
                .toString();
    }
}

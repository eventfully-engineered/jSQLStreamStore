package jsqlstreamstore.streams;

import com.google.common.base.MoreObjects;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.UUID;

public class StreamMessage {

    private final String streamName;
    private final long position;
    private final LocalDateTime createdUtc;
    private final UUID messageId;
    private final String metadata;
    private final long streamVersion;
    private final String type;
    private final GetJsonData getJsonData;

    /**
     *
     * @param streamName
     * @param messageId
     * @param streamVersion
     * @param position
     * @param createdUtc
     * @param type
     * @param metadata
     * @param data
     */
    public StreamMessage(String streamName,
                         UUID messageId,
                         long streamVersion,
                         long position,
                         LocalDateTime createdUtc,
                         String type,
                         String metadata,
                         String data) {
        this(streamName, messageId, streamVersion, position, createdUtc, type, metadata, () -> data);
    }

    /**
     *
     * @param streamName
     * @param messageId
     * @param streamVersion
     * @param position
     * @param createdUtc
     * @param type
     * @param metadata
     * @param getData
     */
    public StreamMessage(String streamName,
                         UUID messageId,
                         long streamVersion,
                         long position,
                         LocalDateTime createdUtc,
                         String type,
                         String metadata,
                         GetJsonData getData) {
        this.streamName = streamName;
        this.messageId = messageId;
        this.streamVersion = streamVersion;
        this.position = position;
        this.createdUtc = createdUtc;
        this.type = type;
        this.metadata = metadata;
        this.getJsonData = getData;
    }

    public long getPosition() {
        return position;
    }

    public LocalDateTime getCreatedUtc() {
        return createdUtc;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getMetadata() {
        return metadata;
    }

    public long getStreamVersion() {
        return streamVersion;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getType() {
        return type;
    }

    public String getJsonData() throws SQLException {
        return getJsonData.get();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("messageId", getMessageId())
                .add("streamName", getStreamName())
                .add("streamVersion", getStreamVersion())
                .add("position", getPosition())
                .add("type", getType())
                .toString();
    }
}

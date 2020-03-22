package jsqlstreamstore.streams;

import com.google.common.base.MoreObjects;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.UUID;

public class StreamMessage {

    private final long position;
    private final LocalDateTime createdUtc;
    private final UUID messageId;
    private final String jsonMetadata;
    private final int streamVersion;
    private final String streamId;
    private final String type;
    private final GetJsonData getJsonData;

    /**
     *
     * @param streamId
     * @param messageId
     * @param streamVersion
     * @param position
     * @param createdUtc
     * @param type
     * @param jsonMetadata
     * @param jsonData
     */
    public StreamMessage(String streamId,
                         UUID messageId,
                         int streamVersion,
                         long position,
                         LocalDateTime createdUtc,
                         String type,
                         String jsonMetadata,
                         String jsonData) {
        this(streamId, messageId, streamVersion, position, createdUtc, type, jsonMetadata, () -> jsonData);
    }

    /**
     *
     * @param streamId
     * @param messageId
     * @param streamVersion
     * @param position
     * @param createdUtc
     * @param type
     * @param jsonMetadata
     * @param getJsonData
     */
    public StreamMessage(String streamId,
                         UUID messageId,
                         int streamVersion,
                         long position,
                         LocalDateTime createdUtc,
                         String type,
                         String jsonMetadata,
                         GetJsonData getJsonData) {
        this.streamId = streamId;
        this.messageId = messageId;
        this.streamVersion = streamVersion;
        this.position = position;
        this.createdUtc = createdUtc;
        this.type = type;
        this.jsonMetadata = jsonMetadata;
        this.getJsonData = getJsonData;
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

    public String getJsonMetadata() {
        return jsonMetadata;
    }

    public int getStreamVersion() {
        return streamVersion;
    }

    public String getStreamId() {
        return streamId;
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
                .add("streamId", getStreamId())
                .add("streamVersion", getStreamVersion())
                .add("position", getPosition())
                .add("type", getType())
                .toString();
    }
}

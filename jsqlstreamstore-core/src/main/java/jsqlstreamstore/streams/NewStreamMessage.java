package jsqlstreamstore.streams;

import com.google.common.base.MoreObjects;
import jsqlstreamstore.infrastructure.Ensure;

import java.util.UUID;

/**
 *
 *
 */
public class NewStreamMessage {

    private final UUID messageId;
    private final String type;
    private final String jsonData;
    private final String jsonMetadata;

    /**
     *
     * @param messageId
     * @param type
     * @param jsonData
     */
    public NewStreamMessage(UUID messageId, String type, String jsonData) {
        this(messageId, type, jsonData, "");
    }

    /**
     *
     * @param messageId
     * @param type
     * @param jsonData
     * @param jsonMetadata
     */
    public NewStreamMessage(UUID messageId, String type, String jsonData, String jsonMetadata) {
        Ensure.notNull(messageId);
        Ensure.notNullOrEmpty(type, "type");
        Ensure.notNullOrEmpty(jsonData, "data");

        this.messageId = messageId;
        this.type = type;
        this.jsonData = jsonData;
        this.jsonMetadata = jsonMetadata == null ? "" : jsonMetadata;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getType() {
        return type;
    }

    public String getJsonData() {
        return jsonData;
    }

    public String getJsonMetadata() {
        return jsonMetadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("messageId", getMessageId())
                .add("type", getType())
                .toString();
    }
}

package com.eventfullyengineered.jsqlstreamstore.streams;

import com.eventfullyengineered.jsqlstreamstore.infrastructure.Ensure;
import com.google.common.base.MoreObjects;

import java.util.UUID;

/**
 *
 *
 */
public class NewStreamMessage {

    private final UUID messageId;
    private final String type;
    private final String data;
    private final String metadata;

    /**
     *
     * @param messageId
     * @param type
     * @param data
     */
    public NewStreamMessage(UUID messageId, String type, String data) {
        this(messageId, type, data, null);
    }

    /**
     *
     * @param messageId
     * @param type
     * @param data
     * @param metadata
     */
    public NewStreamMessage(UUID messageId, String type, String data, String metadata) {
        this.messageId = Ensure.notNull(messageId);
        this.type = Ensure.notNullOrEmpty(type, "type");
        this.data = Ensure.notNullOrEmpty(data, "data");
        this.metadata = metadata;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getType() {
        return type;
    }

    public String getData() {
        return data;
    }

    public String getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("messageId", getMessageId())
                .add("type", getType())
                .toString();
    }
}

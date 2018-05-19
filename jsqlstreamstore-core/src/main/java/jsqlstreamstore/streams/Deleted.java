package jsqlstreamstore.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;

import java.util.UUID;

/**
 * Represents information regarding deleted messages and streams.
 * 
 */
public class Deleted {

    /**
     * The Id of the stream that delete messages are written to.
     */
    public static final String DELETED_STREAM_ID = "$deleted";

    /**
     * The message type of a stream deleted message.
     */
    public static final String STREAM_DELETED_MESSAGE_TYPE = "$stream-deleted";

    /**
     * The message type of a message deleted message. 
     */
    public static final String MESSAGE_DELETED_MESSAGE_TYPE = "$message-deleted";
    
    public static final SqlStreamId DELETED_SQL_STREAM_ID = new SqlStreamId(DELETED_STREAM_ID);
    
    
    private Deleted() {
        // static constants only
    }
    
    /**
     * Creates {@link NewStreamMessage} that contains a stream deleted message.
     * @param streamId The stream id of the deleted stream.
     * @return
     */
    public static NewStreamMessage createStreamDeletedMessage(String streamId) {
        try {
            StreamDeleted streamDeleted = new StreamDeleted(streamId);
            // TODO: get objectmapper from some global configuration/singleton
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(streamDeleted);

            return new NewStreamMessage(Generators.timeBasedGenerator().generate(), STREAM_DELETED_MESSAGE_TYPE, json);
        } catch (JsonProcessingException ex) {
            // TODO: throw custom exception or throw the checked exception
            throw new RuntimeException(ex);
        }
    }

    /**
     * Creates a {@link NewStreamMessage} that contains a message deleted message.
     * @param streamId The stream id of the deleted stream.
     * @param messageId The message id of the deleted message.
     * @return A {@link NewStreamMessage}
     */
    public static NewStreamMessage createMessageDeletedMessage(String streamId, UUID messageId) {
        try {            
            MessageDeleted messageDeleted = new MessageDeleted(streamId, messageId);
            // TODO: get objectmapper from some global configuration/singleton
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(messageDeleted);
            return new NewStreamMessage(Generators.timeBasedGenerator().generate(), MESSAGE_DELETED_MESSAGE_TYPE, json);
        } catch (JsonProcessingException ex) {
            // TODO: throw custom exception or throw the checked exception
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * The message appended to $deleted when a stream is deleted.
     * 
     */
    public static class StreamDeleted {
        
        /**
         * The stream id the deleted of the deleted stream.
         */
        private final String streamId;
        
        public StreamDeleted(String streamId) {
            this.streamId = streamId;
        }
        
        public String getStreamId() {
            return streamId;
        }
    }

    /**
     * The message appended to $deleted with an individual message is deleted.
     * 
     */
    public static class MessageDeleted {
        
        /**
         * The stream id the deleted message belonged to.
         */
        private final String streamId;

        /**
         * The message id of the deleted message.
         */
        private final UUID messageId;
        
        public MessageDeleted(String streamId, UUID messageId) {
            this.streamId = streamId;
            this.messageId = messageId;
        }

        public String getStreamId() {
            return streamId;
        }

        public UUID getMessageId() {
            return messageId;
        }
        
    }

}

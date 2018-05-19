package jsqlstreamstore.infrastructure.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import java.io.IOException;

/**
 *
 *
 */
public class JacksonSerializer implements JsonSerializerStrategy {

    public static final JacksonSerializer DEFAULT = new JacksonSerializer(new ObjectMapper());

    private final ObjectMapper objectMapper;

    /**
     *
     * @param objectMapper
     */
    public JacksonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     *
     */
    @Override
    public String toJson(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    /**
     *
     */
    @Override
    public <T> T fromJson(String s, Class<T> type) {
        try {
            return Strings.isNullOrEmpty(s) ? null : objectMapper.readValue(s, type);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

}

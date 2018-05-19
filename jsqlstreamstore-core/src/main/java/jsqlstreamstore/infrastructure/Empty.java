package jsqlstreamstore.infrastructure;

import jsqlstreamstore.streams.StreamMessage;

import java.util.Collections;
import java.util.Map;

// TODO: From EventStore...not sure we need this but I do kinda like it
public final class Empty {

    public static final byte[] BYTE_ARRAY = new byte[0];
    public static final String[] STRING_ARRAY = new String[0];
    public static final StreamMessage[] STREAM_MESSAGE = new StreamMessage[0];
    public static final Map<String, Object> CUSTOM_STREAM_METADATA = Collections.emptyMap();

    private Empty() {
        // statics only
    }
}

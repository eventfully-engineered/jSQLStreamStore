package jsqlstreamstore.streams;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Value object which represents a valid Stream Id. Not sure we need both this and SqlStreamId
 */
public class StreamId {

    private final String value;

    public StreamId(String value) {
        Preconditions.checkNotNull(value);
        if (StringUtils.containsWhitespace(value)) {
            throw new IllegalArgumentException("value must not contain whitespace");
        }
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        Objects.equals(this, obj);
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StreamId other = (StreamId) obj;
        return value.equals(other.value);
    }


}

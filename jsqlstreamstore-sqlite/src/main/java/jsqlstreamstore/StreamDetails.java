package jsqlstreamstore;

import com.google.common.base.Preconditions;

public class StreamDetails {

    private int id;
    private String name;
    private long version;
    private long position;
    private Long maxAge;
    private Long maxCount;

    public StreamDetails(int id, String name, long version, long position, Long maxAge, Long maxCount) {
        this.id = id;
        this.name = Preconditions.checkNotNull(name);
        this.version = version;
        this.position = position;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getVersion() {
        return version;
    }

    public long getPosition() {
        return position;
    }

    public Long getMaxAge() {
        return maxAge;
    }

    public Long getMaxCount() {
        return maxCount;
    }

}

package com.eventfullyengineered.jsqlstreamstore.sqlite;

import com.google.common.base.Preconditions;

public class StreamDetails {

    private Integer id;
    private String name;
    private long version;
    private long position;
    private Long maxAge;
    private Long maxCount;

    public StreamDetails(Integer id, String name, long version, long position, Long maxAge, Long maxCount) {
        this.id = id;
        this.name = Preconditions.checkNotNull(name);
        this.version = version;
        this.position = position;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
    }

    public Integer getId() {
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


    // TODO: I dont like this...
    public boolean isEmpty() {
        return id == null && version == -1;
    }

    public static StreamDetails empty(String streamName) {
        return new StreamDetails(null, streamName, -1, -1, null, null);
    }

}

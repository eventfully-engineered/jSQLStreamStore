package com.eventfullyengineered.jsqlstreamstore.streams;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.eventfullyengineered.jsqlstreamstore.infrastructure.Utils;

import java.nio.charset.StandardCharsets;

public class SqlStreamId {

    // TODO: what to do with this....
    //public static final SqlStreamId DELETED = new SqlStreamId(Deleted.DELETED_STREAM_ID);
    //public static final SqlStreamId DELETED = new StreamIdInfo(Deleted.DELETED_STREAM_ID).getSqlStreamId();

    private final String id;
    private final String originalId;

    public SqlStreamId(String originalId) {
        this.originalId = Preconditions.checkNotNull(originalId);

        id = Utils.isUUID(originalId)
            ? originalId
            // TODO: can we just to .toString()?
            : Hashing.murmur3_128().hashString(originalId, StandardCharsets.UTF_8).toString()
            .replaceAll("-", "");
    }

    private SqlStreamId(String originalId, String id) {
        this.originalId = originalId;
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getOriginalId() {
        return originalId;
    }

    // TODO: rather than having StreamIdInfo how about just have a method here
    // to get metadata streamId?
    // also...perhaps a convenience method to get metadata stream id without the need
    // to first create a SqlStreamId?
    public SqlStreamId getMetadataSqlStreamId() {
        return new SqlStreamId("$$" + id, "$$" + originalId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", getId())
            .add("originalId", getOriginalId())
            .toString();
    }

}

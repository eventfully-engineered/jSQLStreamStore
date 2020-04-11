package jsqlstreamstore.streams;

import com.google.common.base.MoreObjects;

import java.math.BigInteger;

/**
 * From SqlStreamStore MetadataMessage is retrieved from stream jsqlstreamstore.store (db) and then passed to
 * StreamMetadataResult (https://github.com/damianh/SqlStreamStore/blob/ebbb973959729c98836b5f95c3b55ea60eb6ef7a/src/SqlStreamStore.MsSql/MsSqlStreamStore.StreamMetadata.cs)
 *
 * This vs StreamMetadata which is from eventstore
 *
 *
 */
public class MetadataMessage {

    // TODO: should be streamName

    /**
     * The stream id
     */
	private String streamId;

	/**
	 * The max age of messages retained in the stream
	 */
	private Integer maxAge;

	/**
	 * The max count of messages retained in the stream
	 */
	private Long maxCount;

	/**
	 * Custom json
	 */
	private String metaJson;

	public MetadataMessage() {}

	public MetadataMessage(String streamId, Integer maxAge, Long maxCount, String metaJson) {
	    this.streamId = streamId;
	    this.maxAge = maxAge;
	    this.maxCount = maxCount;
	    this.metaJson = metaJson;
	}

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public Integer getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(Integer maxAge) {
        this.maxAge = maxAge;
    }

    public Long getMaxCount() {
        return maxCount;
    }

    public void setMaxCount(Long maxCount) {
        this.maxCount = maxCount;
    }

    public String getMetaJson() {
        return metaJson;
    }

    public void setMetaJson(String metaJson) {
        this.metaJson = metaJson;
    }

	@Override
	public String toString() {
	    return MoreObjects.toStringHelper(this)
	            .add("streamId", getStreamId())
	            .add("maxAge", getMaxAge())
	            .add("maxCount", getMaxCount())
	            .add("metaJson", getMetaJson())
	            .toString();
	}
}

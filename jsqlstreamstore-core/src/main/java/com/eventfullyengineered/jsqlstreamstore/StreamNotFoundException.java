package com.eventfullyengineered.jsqlstreamstore;

public class StreamNotFoundException extends RuntimeException {

	private static final long serialVersionUID = 4782760145300621611L;

    public StreamNotFoundException(String streamName) {
        super(streamName + " was not found");
    }

}

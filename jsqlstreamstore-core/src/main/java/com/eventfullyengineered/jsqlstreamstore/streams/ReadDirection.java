package com.eventfullyengineered.jsqlstreamstore.streams;

/**
 * Represents the direction of a read operation.
 *
 */
public enum ReadDirection {
    /**
     * From the start to the end.
     */
    FORWARD,

    /**
     * From the end to the start.
     */
	BACKWARD
}

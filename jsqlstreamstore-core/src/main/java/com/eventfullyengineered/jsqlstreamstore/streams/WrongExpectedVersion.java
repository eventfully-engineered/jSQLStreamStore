package com.eventfullyengineered.jsqlstreamstore.streams;

/**
 * Represents an exception that is thrown when a version supplied as part of an append does not match the stream version.
 * This is part of concurrency control.
 *
 */
public class WrongExpectedVersion extends RuntimeException {

    private static final long serialVersionUID = 2055098540147895634L;

    public WrongExpectedVersion(String streamName, long expectedVersion) {
        super(ErrorMessages.appendFailedWrongExpectedVersion(streamName, expectedVersion));
    }

    /**
     *
     * @param streamName
     * @param expectedVersion
     * @param innerException
     */
    public WrongExpectedVersion(String streamName, long expectedVersion, Exception innerException) {
        super(ErrorMessages.appendFailedWrongExpectedVersion(streamName, expectedVersion), innerException);
    }

}

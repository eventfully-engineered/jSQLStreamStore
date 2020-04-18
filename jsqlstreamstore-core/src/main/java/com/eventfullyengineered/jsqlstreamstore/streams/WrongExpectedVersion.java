package com.eventfullyengineered.jsqlstreamstore.streams;

/**
 * Represents an exception that is thrown when a version supplied as part of an append does not match the stream version.
 * This is part of concurrency control.
 *
 */
public class WrongExpectedVersion extends RuntimeException {

    private static final long serialVersionUID = 2055098540147895634L;

    // TODO: for postgres...right now dont have a great way to get stream version
    public WrongExpectedVersion(String streamName, long expectedVersion, Exception innerException) {
        super(ErrorMessages.appendFailedWrongExpectedVersion(streamName, expectedVersion), innerException);
    }

    public WrongExpectedVersion(String streamName, long version, long expectedVersion) {
        super(ErrorMessages.appendFailedWrongExpectedVersion(streamName, version, expectedVersion));
    }

    /**
     *
     * @param streamName
     * @param expectedVersion
     * @param innerException
     */
    public WrongExpectedVersion(String streamName, long version, long expectedVersion, Exception innerException) {
        super(ErrorMessages.appendFailedWrongExpectedVersion(streamName, version, expectedVersion), innerException);
    }

}

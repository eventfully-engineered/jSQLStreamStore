package com.eventfullyengineered.jsqlstreamstore.streams;

// TODO: may not need this and could move messages into corresponding exception

/**
 *
 */
public final class ErrorMessages {

    private ErrorMessages() {
        // statics only
    }

//    RAISE EXCEPTION
//        'Wrong expected version: % (Stream: %, Stream Version: %)',
//    write_message.expected_version,
//    write_message.stream_name,
//    _stream_version;

    // TODO: for postgres...right now dont have a great way to get stream version
    public static String appendFailedWrongExpectedVersion(String streamName, long expectedVersion) {
        return String.format("Append failed due to WrongExpectedVersion: %s, Stream: %s", expectedVersion, streamName);
    }

    public static String appendFailedWrongExpectedVersion(String streamName, long version, long expectedVersion) {
        return String.format("Append failed due to WrongExpectedVersion: %s, Stream: %s, Stream version: %s", expectedVersion, streamName, version);
    }

    public static String deleteStreamFailedWrongExpectedVersion(String streamName, long version, long expectedVersion) {
        return String.format("Delete stream failed due to WrongExpectedVersion: %s, Stream: %s, Stream version: %s", expectedVersion, streamName, version);
    }

}

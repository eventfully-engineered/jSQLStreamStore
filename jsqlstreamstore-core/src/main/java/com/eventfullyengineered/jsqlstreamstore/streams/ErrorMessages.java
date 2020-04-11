package com.eventfullyengineered.jsqlstreamstore.streams;

public final class ErrorMessages {

    private ErrorMessages() {
        // statics only
    }

//    RAISE EXCEPTION
//        'Wrong expected version: % (Stream: %, Stream Version: %)',
//    write_message.expected_version,
//    write_message.stream_name,
//    _stream_version;

    public static String appendFailedWrongExpectedVersion(String streamId, long expectedVersion) {
        return String.format("Append failed due to WrongExpectedVersion.Stream: %s, Expected version: %s", streamId, expectedVersion);
    }

    public static String deleteStreamFailedWrongExpectedVersion(String streamId, long expectedVersion) {
        return String.format("Delete stream failed due to WrongExpectedVersion.Stream: %s, Expected version: %s", streamId, expectedVersion);
    }

}

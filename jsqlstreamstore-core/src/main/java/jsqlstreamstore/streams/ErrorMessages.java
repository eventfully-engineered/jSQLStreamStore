package jsqlstreamstore.streams;

public class ErrorMessages {

    private ErrorMessages() {
        // statics only
    }

    public static String appendFailedWrongExpectedVersion(String streamId, long expectedVersion) {
        return String.format("Append failed due to WrongExpectedVersion.Stream: %s, Expected version: %s", streamId, expectedVersion);
    }

    public static String deleteStreamFailedWrongExpectedVersion(String streamId, long expectedVersion) {
        return String.format("Delete stream failed due to WrongExpectedVersion.Stream: %s, Expected version: %s", streamId, expectedVersion);
    }

}

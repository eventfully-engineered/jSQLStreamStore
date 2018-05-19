package jsqlstreamstore.streams;

/**
 * Represents an exception that is thrown when a version supplied as part of an append does not match the stream version.
 * This is part of concurrency control.
 *
 */
public class WrongExpectedVersion extends RuntimeException {

    private static final long serialVersionUID = 2055098540147895634L;

    /**
     *
     * @param message
     */
    public WrongExpectedVersion(String message) {
        super(message);
    }

    /**
     *
     * @param message
     * @param innerException
     */
    public WrongExpectedVersion(String message, Exception innerException) {
        super(message, innerException);
    }
}

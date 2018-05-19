package jsqlstreamstore.infrastructure.serialization;

/**
 *
 *
 */
public class SerializationException extends RuntimeException {

    private static final long serialVersionUID = -535308618677633174L;

    public SerializationException(Exception ex) {
        super(ex);
    }
}

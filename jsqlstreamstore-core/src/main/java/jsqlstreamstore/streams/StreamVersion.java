package jsqlstreamstore.streams;

// Is this the same as EventStore StreamPosition - src/EventStore.ClientAPI
// SqlStreamStore also has a Position class. What is the difference?

/**
 * 
 *
 */
public class StreamVersion {

	/**
	 * The first message in a stream
	 */
    public static final int START = 0;

    /**
     * the last message in a stream
     */
    public static final int END = -1;
    
    private StreamVersion() {
        // static constants only
    }
}

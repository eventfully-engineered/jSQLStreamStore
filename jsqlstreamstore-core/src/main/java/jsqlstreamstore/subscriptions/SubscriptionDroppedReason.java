package jsqlstreamstore.subscriptions;

public enum SubscriptionDroppedReason {

    /**
     * The subscription was disposed deliberately.
     * The associated exception will be null.
     * You will not usually perform any actions as a result of this.
     */
    DISPOSED,

    /**
     * The subscription encountered an error in the subscriber callback.
     * It is your responsibility to check the exception to determine whether it
     * is recoverable, and if so, recreate the subscription if desired.
     */
    SUBSCRIBER_ERROR,

    /**
     * The subscription encountered an error from the underlying StreamStore.
     * It is your responsibility to check the exception to determine whether it
     * is recoverable, and if so, recreate the subscription if desired.
     */
    STREAM_STORE_ERROR
}

package jsqlstreamstore.subscriptions;

@FunctionalInterface
public interface SubscriptionDropped {
    void get(StreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception);
}

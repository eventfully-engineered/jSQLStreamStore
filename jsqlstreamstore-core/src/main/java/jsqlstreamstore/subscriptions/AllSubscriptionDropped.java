package jsqlstreamstore.subscriptions;

@FunctionalInterface
public interface AllSubscriptionDropped {

    //public delegate void AllSubscriptionDropped(IAllStreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception = null);
    void get(AllStreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception);

}

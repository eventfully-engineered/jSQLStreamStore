package jsqlstreamstore.subscriptions;

public interface AllStreamSubscription {

    String getName();
    
    Long getLastPosition();
    
    // Task started();
    
    int getMaxCountPerRead();
    
}

package jsqlstreamstore.subscriptions;

import jsqlstreamstore.streams.StreamMessage;

@FunctionalInterface
public interface AllStreamMessageReceived {

    void get(AllStreamSubscription subscription, StreamMessage streamMessage);

}

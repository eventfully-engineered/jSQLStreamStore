package jsqlstreamstore.subscriptions;

import jsqlstreamstore.streams.StreamMessage;

@FunctionalInterface
public interface StreamMessageReceived {

    void get(StreamSubscription subscription, StreamMessage streamMessage);

}

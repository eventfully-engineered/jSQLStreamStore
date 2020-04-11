package com.eventfullyengineered.jsqlstreamstore.subscriptions;

import com.eventfullyengineered.jsqlstreamstore.streams.StreamMessage;

@FunctionalInterface
public interface StreamMessageReceived {

    void get(StreamSubscription subscription, StreamMessage streamMessage);

}

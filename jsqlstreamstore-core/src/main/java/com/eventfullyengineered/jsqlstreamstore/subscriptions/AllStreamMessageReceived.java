package com.eventfullyengineered.jsqlstreamstore.subscriptions;

import com.eventfullyengineered.jsqlstreamstore.streams.StreamMessage;

@FunctionalInterface
public interface AllStreamMessageReceived {

    void get(AllStreamSubscription subscription, StreamMessage streamMessage);

}

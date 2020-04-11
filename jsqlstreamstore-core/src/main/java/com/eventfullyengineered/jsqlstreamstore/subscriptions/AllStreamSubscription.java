package com.eventfullyengineered.jsqlstreamstore.subscriptions;

import java.io.Closeable;

// TODO: do we want to extend Closable?
public interface AllStreamSubscription extends Closeable {

    String getName();

    Long getLastPosition();

    // Task started();

    int getMaxCountPerRead();

}

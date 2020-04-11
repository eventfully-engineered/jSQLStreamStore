package com.eventfullyengineered.jsqlstreamstore.subscriptions;

import java.io.Closeable;

// TODO: do we want to extend Closable?
public interface StreamSubscription extends Closeable {

    String getName();

    String getStreamId();

    Long getLastVersion();

    // Task started();

    int getMaxCountPerRead();
}

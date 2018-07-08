package jsqlstreamstore.subscriptions;

import java.io.Closeable;

// TODO: do we want to extend Closable?
public interface StreamSubscription extends Closeable {

    String getName();

    String getStreamId();

    Integer getLastVersion();

    // Task started();

    int getMaxCountPerRead();
}

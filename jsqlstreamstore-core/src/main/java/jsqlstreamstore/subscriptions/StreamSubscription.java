package jsqlstreamstore.subscriptions;

import java.io.Closeable;

public interface StreamSubscription extends Closeable {

    String getName();

    String getStreamId();

    Integer getLastVersion();

    // Task started();

    int getMaxCountPerRead();
}

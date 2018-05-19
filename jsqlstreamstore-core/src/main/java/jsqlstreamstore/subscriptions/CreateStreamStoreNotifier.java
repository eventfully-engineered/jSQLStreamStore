package jsqlstreamstore.subscriptions;


import jsqlstreamstore.store.IReadOnlyStreamStore;

// TODO: not sure if this is really necessary
@FunctionalInterface
public interface CreateStreamStoreNotifier {

    // public delegate IStreamStoreNotifier CreateStreamStoreNotifier(IReadonlyStreamStore readonlyStreamStore);
    StreamStoreNotifier createStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore);
}

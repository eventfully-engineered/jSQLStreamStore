package jsqlstreamstore.subscriptions;

import io.reactivex.ObservableSource;

/**
 *  Represents an notifier lets subscribers know that the stream store has new messages.
 */
// TODO: should this use ObservableSource, which doesn't allow for back-pressure, to something that does?
// Perhaps something like Flowable or Publisher?
public interface StreamStoreNotifier extends ObservableSource<Long> {

}

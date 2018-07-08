package jsqlstreamstore.subscriptions;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import jsqlstreamstore.store.IReadOnlyStreamStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An implementation of {@link StreamStoreNotifier} that polls the target stream store for new message.
 */
public class PollingStreamStoreNotifier implements StreamStoreNotifier {

    private static final Logger LOG = LoggerFactory.getLogger("PollingStreamStoreNotifier");

    private final Supplier<Long> _readHeadPosition;
    // TODO: not sure if this is the best way
    // TODO: do we need to dispose of either of these...they dont implement disposable so likely no?
    private final PublishSubject<Long> _storeAppended = PublishSubject.create();
    private final Observable<Long> _appended;

    /**
     * Initializes a new instance of @{link PollingStreamStoreNotifier} with a default interval to poll of 1000 milliseconds
     * @see #PollingStreamStoreNotifier(Supplier, long)
     * @param readonlyStreamStore The store to poll
     */
    public PollingStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore) {
        this(() -> readonlyStreamStore.readHeadPosition(), 1000);
    }

    /**
     * Initializes a new instance of @{link PollingStreamStoreNotifier}
     * @param readHeadPosition An operation to read the head position of a store
     * @param interval The interval to poll in milliseconds
     */
    public PollingStreamStoreNotifier(Supplier<Long> readHeadPosition, long interval) {
        this(readHeadPosition, interval, Schedulers.newThread());
    }

    /**
     * Initializes a new instance of @{link PollingStreamStoreNotifier}
     * @param readHeadPosition An operation to read the head position of a store
     * @param interval The interval to poll in milliseconds
     * @param scheduler the Scheduler to use for scheduling the items
     */
    public PollingStreamStoreNotifier(Supplier<Long> readHeadPosition, long interval, Scheduler scheduler) {
        this._readHeadPosition = readHeadPosition;
        // should we use subscribeOn?
        this._appended = Observable.interval(interval, TimeUnit.MILLISECONDS, scheduler).map(tick -> poll());
        this._appended.subscribe(_storeAppended);
    }

    @Override
    public void subscribe(Observer<? super Long> observer) {
        _storeAppended.subscribe(observer);
    }

    // TODO probably doesnt need to return a Long
    private Long poll() {
        long headPosition = -1;
        long previousHeadPosition = headPosition;
        while (true) {
            try {
                headPosition = _readHeadPosition.get();
                LOG.trace("Polling head position {}. Previous {}", headPosition, previousHeadPosition);
            } catch (Exception ex) {
                LOG.error("Exception occurred polling stream store for messages. HeadPosition {}", headPosition, ex);
            }

            if (headPosition > previousHeadPosition) {
                _storeAppended.onNext(headPosition);
                previousHeadPosition = headPosition;
            }
            return previousHeadPosition;
        }

    }

}

package jsqlstreamstore.subscriptions;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import jsqlstreamstore.store.IReadOnlyStreamStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An implementation of {@link StreamStoreNotifier} that polls the target stream store for new message.
 */
public class PollingStreamStoreNotifier implements StreamStoreNotifier {
    private static final Logger LOG = LoggerFactory.getLogger("PollingStreamStoreNotifier");

    private final Supplier<Long> _readHeadPosition;
    // TODO: not sure if this is the best way
    private final PublishSubject<Long> _storeAppended = PublishSubject.create();
    private final Observable<Long> _appended;

    // https://www.nurkiewicz.com/2017/09/fixed-rate-vs-fixed-delay-rxjava-faq.html
    // https://github.com/ReactiveX/RxJava/issues/448

    /**
     *
     * @param readonlyStreamStore
     */
    public PollingStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore) {
        this(() -> readonlyStreamStore.readHeadPosition(), 1000);
    }

    /**
     *
     * @param readHeadPosition
     * @param interval
     */
    public PollingStreamStoreNotifier(Supplier<Long> readHeadPosition, long interval) {
        this(readHeadPosition, interval, Schedulers.newThread());
    }

    /**
     *
     * @param readHeadPosition
     * @param interval
     * @param scheduler
     */
    public PollingStreamStoreNotifier(Supplier<Long> readHeadPosition, long interval, Scheduler scheduler) {
        this._readHeadPosition = readHeadPosition;
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

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

public class PollingStreamStoreNotifier implements StreamStoreNotifier {
    private static final Logger LOG = LoggerFactory.getLogger("PollingStreamStoreNotifier");

    private final Supplier<Long> _readHeadPosition;
    private final PublishSubject<Long> _storeAppended = PublishSubject.create();
    private final Observable<Long> _appended;

    // https://www.nurkiewicz.com/2017/09/fixed-rate-vs-fixed-delay-rxjava-faq.html
    // https://github.com/ReactiveX/RxJava/issues/448

    public PollingStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore) {
        this(readonlyStreamStore, 1000, Executors.newSingleThreadScheduledExecutor());
    }

    public PollingStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore, long interval, ScheduledExecutorService executorService) {
        this(() -> readonlyStreamStore.readHeadPosition(), interval);
    }

    public PollingStreamStoreNotifier(Supplier<Long> readHeadPosition, long interval) {
        this._readHeadPosition = readHeadPosition;
//        this._appended = Observable.fromCallable(() -> poll())
//            .interval(interval, TimeUnit.MILLISECONDS, Schedulers.newThread());
        //this._appended = Observable.fromCallable(() -> poll()).repeatWhen(o -> o.concatMap(v -> Observable.timer(interval, TimeUnit.MILLISECONDS, Schedulers.newThread())));
        this._appended = Observable.interval(interval, TimeUnit.MILLISECONDS, Schedulers.newThread()).map(tick -> poll());
        this._appended.subscribe(_storeAppended);
        
//        Observable.interval(interval, TimeUnit.MILLISECONDS, Schedulers.newThread()).map(tick -> poll()).subscribe(System.out::println);
//        _storeAppended.interval(interval, TimeUnit.MILLISECONDS, Schedulers.newThread()).map(tick -> poll());
//        _storeAppended.interval(interval, TimeUnit.MILLISECONDS, Schedulers.newThread()).repeatWhen(o -> o.concatMap(v -> Observable.timer(interval, TimeUnit.MILLISECONDS, Schedulers.newThread())));
    }

    public PollingStreamStoreNotifier(Supplier<Long> readHeadPosition, long interval, Scheduler scheduler) {
        this._readHeadPosition = readHeadPosition;
        this._appended = Observable.interval(interval, TimeUnit.MILLISECONDS, scheduler).map(tick -> poll());

//        Schedulers.newThread().schedulePeriodicallyDirect({
//            observer.onNext("application-state-from-network");
//        }, 0, 1000, TimeUnit.MILLISECONDS);
    }


    @Override
    public void subscribe(Observer<? super Long> observer) {
        _storeAppended.subscribe(observer);
//        _appended.subscribe(observer);
    }


    // TODO: turn readHeadPosition to a supplier or a rxjava task
    // TODO: should this return something?
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
            } else {

                // await Task.Delay(_interval, _disposed.Token);
            }
            return previousHeadPosition;
        }

    }

}

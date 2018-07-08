package jsqlstreamstore;

import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.observers.TestObserver;
import jsqlstreamstore.subscriptions.PollingStreamStoreNotifier;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class PollingStreamStoreNotifierTests {

    @Test
    public void whenExceptionOccursReadingHeadPositionThenPollingShouldStillContinue() {
        AtomicLong readHeadCount = new AtomicLong(0L);
        Supplier<Long> readHeadPosition = () -> {
            readHeadCount.incrementAndGet();
            if (readHeadCount.get() % 2 == 0) {
                throw new RuntimeException("oops");
            }
            return readHeadCount.get();
        };

        AtomicInteger received = new AtomicInteger(0);
        TestObserver to = new TestObserver(new Observer<Long>() {
            @Override
            public void onSubscribe(Disposable d) {
            }

            @Override
            public void onNext(Long aVoid) {
                received.incrementAndGet();
                if (received.get() > 5) {
                    this.onComplete();
                }
            }

            @Override
            public void onError(Throwable e) {
            }

            @Override
            public void onComplete() {
            }

        });

        PollingStreamStoreNotifier notifier = new PollingStreamStoreNotifier(readHeadPosition, 10);
        notifier.subscribe(to);

        to.assertSubscribed();
        to.awaitCount(5);
        to.assertValueCount(5);
        assertEquals(5, received.get());
    }

}

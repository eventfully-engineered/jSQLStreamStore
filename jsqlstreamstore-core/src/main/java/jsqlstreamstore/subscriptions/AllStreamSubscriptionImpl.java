package jsqlstreamstore.subscriptions;

import com.google.common.base.Strings;
import io.reactivex.ObservableSource;
import jsqlstreamstore.store.IReadOnlyStreamStore;
import jsqlstreamstore.streams.Position;
import jsqlstreamstore.streams.ReadAllPage;
import jsqlstreamstore.streams.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: allow users to cancel subscription. Would need to return a disposable handle back to the user or perhaps close
// is enough? Not sure I like the idea of exposing rxjava2's Disposable to the outside world
public class AllStreamSubscriptionImpl implements AllStreamSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(AllStreamSubscriptionImpl.class);

    public static final int DEFAULT_PAGE_SIZE = 10;
    // TODO: allow this to be passed in?
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private int _pageSize = DEFAULT_PAGE_SIZE;
    private long _nextPosition;
    private final IReadOnlyStreamStore _readonlyStreamStore;
    private final AllStreamMessageReceived _streamMessageReceived;
    private final boolean _prefetchJsonData;
    private final HasCaughtUp _hasCaughtUp;
    private final AllSubscriptionDropped _subscriptionDropped;
    //private readonly IDisposable _notification;
    //private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
    //private readonly AsyncAutoResetEvent _streamStoreNotification = new AsyncAutoResetEvent();
    //private readonly TaskCompletionSource<object> _started = new TaskCompletionSource<object>();


    private final AtomicBoolean _notificationRaised = new AtomicBoolean();

    private Long fromPosition;
    private Long lastPosition;
    private String name;

    public AllStreamSubscriptionImpl(
            Long continueAfterPosition,
            IReadOnlyStreamStore readonlyStreamStore,
            ObservableSource streamStoreAppendedNotification,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            boolean prefetchJsonData,
            String name) {

        fromPosition = continueAfterPosition;
        lastPosition = continueAfterPosition;
        _nextPosition = continueAfterPosition == null ? Position.START : continueAfterPosition + 1;
        _readonlyStreamStore = readonlyStreamStore;
        _streamMessageReceived = streamMessageReceived;
        _prefetchJsonData = prefetchJsonData;
        _subscriptionDropped = subscriptionDropped != null ? subscriptionDropped : (__, ___, ____) -> {};
        _hasCaughtUp = hasCaughtUp != null ? hasCaughtUp : __ -> {};
        this.name = Strings.isNullOrEmpty(name) ? UUID.randomUUID().toString() : name;

//        readonlyStreamStore.OnDispose += ReadonlyStreamStoreOnOnDispose;
//
//        _notification = streamStoreAppendedNotification.Subscribe(_ =>
//            {
//                _streamStoreNotification.Set();
//            });
//
//        streamStoreAppendedNotification.subscribe();



//        Task.Run(PullAndPush);
        EXECUTOR.execute(this::pullAndPush);

        LOG.info("AllStream subscription created {} continuing after position {}.",
            name, continueAfterPosition == null ? "<null>" : continueAfterPosition.toString());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Long getLastPosition() {
        return lastPosition;
    }

    @Override
    public int getMaxCountPerRead() {
        return _pageSize;
    }

    public void setMaxCountPerRead(int pageSize) {
        _pageSize = pageSize <= 0 ? 1 : pageSize;
    }


    // async task
    private void pullAndPush() {
        if (fromPosition == Position.END) {
            initialize();
        }
        //_started.SetResult(null);

        while (true) {
            boolean pause = false;
            Boolean lastHasCaughtUp = null;

            while (!pause) {
                ReadAllPage page = pull();

                push(page);

                if ((lastHasCaughtUp == null || lastHasCaughtUp != page.isEnd()) && page.getMessages().length > 0) {
                    // Only raise if the state changes and there were messages read
                    lastHasCaughtUp = page.isEnd();
                    _hasCaughtUp.hasCaughtUp(page.isEnd());
                }

                pause = page.isEnd() && page.getMessages().length == 0;
            }

            // Wait for notification before starting again.
//            try
//            {
//                _streamStoreNotification.WaitAsync(_disposed.Token).NotOnCapturedContext();
//            }
//            catch (TaskCanceledException)
//            {
//                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//                throw;
//            }
        }
    }

    // async task
    private void initialize() {
        ReadAllPage eventsPage;
        try {
            // Get the last stream version and subscribe from there.
            eventsPage = _readonlyStreamStore.readAllBackwards(
                Position.END,
                1,
                false);
        }
//        catch (ObjectDisposedException)
//        {
//            NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//            throw;
//        }
//        catch (OperationCanceledException)
//        {
//            NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//            throw;
//        }
        catch (Exception ex) {
            LOG.error("Error reading stream {}", name, ex);
            notifySubscriptionDropped(SubscriptionDroppedReason.STREAM_STORE_ERROR, ex);
            throw new RuntimeException(ex);
        }

        // Only new Messages, i.e. the one after the current last one.
        // Edge case for empty store where Next position 0 (when FromPosition = 0)
        _nextPosition = eventsPage.getFromPosition() == 0 ? 0 : eventsPage.getFromPosition() + 1;
    }

    // async task
    private ReadAllPage pull() {
        ReadAllPage readAllPage;
        try {
            readAllPage = _readonlyStreamStore.readAllForwards(_nextPosition, getMaxCountPerRead(), _prefetchJsonData);
        }
//        catch(ObjectDisposedException)
//        {
//            NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//            throw;
//        }
//        catch (OperationCanceledException)
//        {
//            NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//            throw;
//        }
        catch (Exception ex) {
            LOG.error("Error reading all stream {}", name, ex);
            notifySubscriptionDropped(SubscriptionDroppedReason.STREAM_STORE_ERROR, ex);
            throw new RuntimeException(ex);
        }
        return readAllPage;
    }

    // async task
    private void push(ReadAllPage page) {
        for (StreamMessage message : page.getMessages()) {
//            if (_disposed.IsCancellationRequested)
//            {
//                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//                _disposed.Token.ThrowIfCancellationRequested();
//            }
            _nextPosition = message.getPosition() + 1;
            lastPosition = message.getPosition();
            try {
                //_streamMessageReceived(this, message);
                _streamMessageReceived.get(this, message);
            } catch (Exception ex) {
                LOG.error("Exception with subscriber receiving message {} Message: {}.", name, message, ex);
                notifySubscriptionDropped(SubscriptionDroppedReason.SUBSCRIBER_ERROR, ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private void notifySubscriptionDropped(SubscriptionDroppedReason reason, Exception exception) {
        if (_notificationRaised.compareAndSet(true, false)) {
            return;
        }
        try {
            LOG.info("All stream subscription dropped {}. Reason: {}", name, reason, exception);
            // _subscriptionDropped.Invoke(this, reason, exception);
            _subscriptionDropped.get(this, reason, exception);
        } catch (Exception ex) {
            LOG.error("Error notifying subscriber that subscription has been dropped ({}).", name, ex);
        }
    }

    @Override
    public void close() throws IOException {
        // TODO: do i need to do any of these?

//        if (_disposed.IsCancellationRequested)
//        {
//            return;
//        }
//        _disposed.Cancel();
//        _notification.Dispose();

        notifySubscriptionDropped(SubscriptionDroppedReason.DISPOSED, null);
    }
}

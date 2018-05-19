package jsqlstreamstore.subscriptions;

import com.google.common.base.Strings;
import jsqlstreamstore.store.IReadOnlyStreamStore;
import jsqlstreamstore.streams.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamSubscriptionImpl implements StreamSubscription {

    /**
     * The default page size to read.
     */
    public static final int DEFAULT_PAGE_SIZE = 10;
    private static final Logger LOG = LoggerFactory.getLogger("StreamSubscriptionImpl");

    private int _pageSize = DEFAULT_PAGE_SIZE;
    private int _nextVersion;
    private Integer _continueAfterVersion;
    private IReadOnlyStreamStore _readonlyStreamStore;
    private StreamMessageReceived _streamMessageReceived;
    private boolean _prefectchJsonData;
    private HasCaughtUp _hasCaughtUp;
    private SubscriptionDropped _subscriptionDropped;
//    private readonly IDisposable _notification;
//    private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
//    private readonly AsyncAutoResetEvent _streamStoreNotification = new AsyncAutoResetEvent();
//    private readonly TaskCompletionSource<object> _started = new TaskCompletionSource<object>();
    private final AtomicBoolean _notificationRaised = new AtomicBoolean();

    private String streamId;
    private String name;
    private Integer lastVersion;

    public StreamSubscriptionImpl(
            String streamId,
            Integer continueAfterVersion,
            IReadOnlyStreamStore readonlyStreamStore,
            //IObservable<Unit> streamStoreAppendedNotification,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            boolean prefectchJsonData,
            String name) {
        this.streamId = streamId;
        _continueAfterVersion = continueAfterVersion;
        _readonlyStreamStore = readonlyStreamStore;
        _streamMessageReceived = streamMessageReceived;
        _prefectchJsonData = prefectchJsonData;
        _subscriptionDropped = subscriptionDropped; // subscriptionDropped ?? ((_, __, ___) => { });
        _hasCaughtUp = hasCaughtUp; // hasCaughtUp ?? (_ => { });
        this.name = Strings.isNullOrEmpty(name) ? UUID.randomUUID().toString() : name;

//        readonlyStreamStore.OnDispose += ReadonlyStreamStoreOnOnDispose;
//        _notification = streamStoreAppendedNotification.Subscribe(_ =>
//            {
//                _streamStoreNotification.Set();
//            });
//
//        Task.Run(PullAndPush);

        LOG.info("Stream subscription created {} continuing after version {}.",
            name, continueAfterVersion == null ? "<null>" : continueAfterVersion.toString());
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

    @Override
    public Integer getLastVersion() {
        return lastVersion;
    }

    @Override
    public int getMaxCountPerRead() {
        return _pageSize;
    }

    public void setMaxCountPerRead(int pageSize) {
        _pageSize = pageSize <= 0 ? 1 : pageSize;
    }


    // async task
    private void PullAndPush()
    {
        if (_continueAfterVersion != null) {
            _nextVersion = 0;
        } else if (_continueAfterVersion == StreamVersion.END) {
            Initialize();
        } else {
            _nextVersion = _continueAfterVersion + 1;
        }

        // _started.SetResult(null);

        while (true) {
            boolean pause = false;
            Boolean lastHasCaughtUp = null;

            while (!pause) {
                ReadStreamPage page = Pull();

                if (page.getStatus() != PageReadStatus.SUCCESS) {
                    break;
                }

                Push(page);

                if (lastHasCaughtUp != null || lastHasCaughtUp != page.isEnd()) {
                    // Only raise if the state changes
                    lastHasCaughtUp = page.isEnd();
//                    _hasCaughtUp(page.isEnd());
                }

                pause = page.isEnd() && page.getMessages().length == 0;
            }

            // Wait for notification before starting again.
//            try
//            {
//                _streamStoreNotification.WaitAsync(_disposed.Token).NotOnCapturedContext();
//            }
//            catch(TaskCanceledException)
//            {
//                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//                throw;
//            }
        }
    }

    // async task
    private void Initialize() {
        ReadStreamPage eventsPage;
        try {
            // Get the last stream version and subscribe from there.
            eventsPage = _readonlyStreamStore.readStreamBackwards(
                streamId,
                StreamVersion.END,
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
            LOG.error("Error reading stream {}/{}", name, streamId, ex);
            notifySubscriptionDropped(SubscriptionDroppedReason.STREAM_STORE_ERROR, ex);
            throw new RuntimeException(ex);
        }

        //Only new Messages, i.e. the one after the current last one
        _nextVersion = eventsPage.getLastStreamVersion() + 1;
        lastVersion = _nextVersion;
    }

    // async task
    private ReadStreamPage Pull() {
        ReadStreamPage readStreamPage;
        try {
            readStreamPage = _readonlyStreamStore.readStreamForwards(streamId, _nextVersion, getMaxCountPerRead(), _prefectchJsonData);
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
            LOG.error("Error reading stream {}/{}", name, streamId, ex);
            notifySubscriptionDropped(SubscriptionDroppedReason.STREAM_STORE_ERROR, ex);
            throw new RuntimeException(ex);
        }
        return readStreamPage;
    }

    // async task
    private void Push(ReadStreamPage page) {
        for (StreamMessage message : page.getMessages()) {
//            if (_disposed.IsCancellationRequested)
//            {
//                NotifySubscriptionDropped(SubscriptionDroppedReason.Disposed);
//                _disposed.Token.ThrowIfCancellationRequested();
//            }
            _nextVersion = message.getStreamVersion() + 1;
            lastVersion = message.getStreamVersion();
            try {
//                _streamMessageReceived(this, message);
            } catch (Exception ex) {
                LOG.error("Exception with subscriber receiving message {}/{} Message: {}.",
                    name, streamId, message, ex);
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
            LOG.info("Subscription dropped {}/{}. Reason: {}", name, streamId, reason, exception);
//            _subscriptionDropped.Invoke(this, reason, exception);
        } catch(Exception ex) {
            LOG.error("Error notifying subscriber that subscription has been dropped ({}/{}).",
                name, streamId, ex);
        }
    }

    @Override
    public void close() throws IOException {
//        if (_disposed.IsCancellationRequested)
//        {
//            return;
//        }
//        _disposed.Cancel();
//        _notification.Dispose();
        notifySubscriptionDropped(SubscriptionDroppedReason.DISPOSED, null);
    }

}

package jsqlstreamstore.subscriptions;

import jsqlstreamstore.store.IReadOnlyStreamStore;

import java.io.IOException;

public class PollingStreamStoreNotifier implements StreamStoreNotifier {

    //private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
    //private readonly Func<CancellationToken, Task<long>> _readHeadPosition;
    //private readonly Subject<Unit> _storeAppended = new Subject<Unit>();

    private final int interval;


    public PollingStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore) {
        this(readonlyStreamStore, 1000);
    }

    public PollingStreamStoreNotifier(IReadOnlyStreamStore readonlyStreamStore, int interval) {
        this.interval = interval;
    }

    // TODO: turn readHeadPosition to a supplier or a rxjava task

//    public PollingStreamStoreNotifier(IReadonlyStreamStore readonlyStreamStore, int interval = 1000)
//    : this(readonlyStreamStore.ReadHeadPosition, interval)
//    {}

//    public PollingStreamStoreNotifier(Func<CancellationToken, Task<long>> readHeadPosition, int interval = 1000)
//    {
//        _readHeadPosition = readHeadPosition;
//        _interval = interval;
//        Task.Run(Poll, _disposed.Token);
//    }

    // async task
    private void poll() {
        long headPosition = -1;
        long previousHeadPosition = headPosition;
//        while (!_disposed.IsCancellationRequested)
//        {
//            try
//            {
//                headPosition = await _readHeadPosition(_disposed.Token);
//                if(s_logger.IsTraceEnabled())
//                {
//                    s_logger.TraceFormat("Polling head position {headPosition}. Previous {previousHeadPosition}",
//                        headPosition, previousHeadPosition);
//                }
//            }
//            catch(Exception ex)
//            {
//                s_logger.ErrorException($"Exception occurred polling stream store for messages. " +
//                    $"HeadPosition: {headPosition}", ex);
//            }
//
//            if(headPosition > previousHeadPosition)
//            {
//                _storeAppended.OnNext(Unit.Default);
//                previousHeadPosition = headPosition;
//            }
//            else
//            {
//                await Task.Delay(_interval, _disposed.Token);
//            }
//        }
    }

    @Override
    public void close() throws IOException {
//        _disposed.Cancel();
    }
}

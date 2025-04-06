using DurableQueue.Repository;
using MessagePack;
using System.Collections.Concurrent;

namespace DurableQueue
{
    public class DurableQueue<T> : IDisposable
    {
        private readonly string _queueName;
        private QueueDb _qDatabase;
        private readonly ConcurrentQueue<T> _queue = new();
        private readonly ConcurrentQueue<T> _qBuffer = new();
        private readonly SemaphoreSlim _qSem = new(1);
        private readonly ManualResetEventSlim _mre = new();
        private readonly CancellationTokenSource _cts = new();

        public DurableQueue(string queueName)
        {
            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException("Queue name cannot be null or empty");

            _queueName = queueName;
            _qDatabase = QueueDb.CreateQueue(queueName, _cts);

            LoadQueueFromDatabase().GetAwaiter().GetResult();

            Task.Factory.StartNew(BufferEnqueueTask, _cts.Token,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _mre.Reset();
            _mre.Dispose();
            _qDatabase.Dispose();
            _qSem.Release();
            _qSem.Dispose();
        }

        private async Task LoadQueueFromDatabase()
        {
            try
            {
                await _qSem.WaitAsync(_cts.Token);

                await foreach (var item in _qDatabase.LoadItemsToMemory())
                {
                    var deserializedItem = MessagePackSerializer.Deserialize<T>(item);
                    _queue.Enqueue(deserializedItem);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to load items from database: {ex.Message}", ex);
            }
            finally
            {
                _qSem.Release();
            }
        }

        public int Count => _queue.Count;

        public string QueueName => _queueName;

        public void Enqueue(T item)
        {
            if (item == null)
                throw new ArgumentNullException("Item cannot be null");

            _qBuffer.Enqueue(item);
            _mre.Set();
        }

        private async Task BufferEnqueueTask()
        {
            var buffer = new List<T>();

            while (!_cts.IsCancellationRequested)
            {
                if (_qBuffer.Count == 0)
                    _mre.Wait(_cts.Token);

                var bSize = 0;
                buffer.Clear();

                bool hasItems;

                do
                {
                    hasItems = _qBuffer.TryDequeue(out T? item);

                    if (hasItems)
                    {
                        if (item != null)
                            buffer.Add(item);
                        bSize++;
                    }
                }
                while (bSize < 100_000 && hasItems && !_cts.IsCancellationRequested);

                Console.WriteLine($"Buffer size: {buffer.Count}");
                await BulkEnqueue(buffer);

                _mre.Reset();
            }
        }

        private async Task BulkEnqueue(IEnumerable<T> items)
        {
            if (items == null || !items.Any())
                throw new ArgumentNullException("Item cannot be null");

            try
            {
                _qSem.Wait(_cts.Token);

                var byteList = new List<byte[]>();

                foreach (var item in items)
                {
                    var bytes = MessagePackSerializer.Serialize(item);
                    byteList.Add(bytes);
                }

                await _qDatabase.Enqueue(byteList);

                foreach (var item in items)
                {
                    _queue.Enqueue(item);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to enqueue item: {ex.Message}", ex);
            }
            finally
            {
                _qSem.Release();
            }
        }

        public async Task<IEnumerable<T?>> Dequeue(int cnt = 1)
        {
            try
            {
                await _qSem.WaitAsync(_cts.Token);

                var retList = new List<T?>();

                do
                {
                    if (_queue.TryDequeue(out T? qItem))
                    {
                        retList.Add(qItem);
                    }
                }
                while (_queue.Count > 0 && retList.Count < cnt);

                if (retList.Count != 0)
                    await _qDatabase.Delete(retList.Count);

                return retList;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to dequeue item: {ex.Message}", ex);
            }
            finally
            {
                _qSem.Release();
            }
        }
    }
}

using DurableQueue.Repository;
using MessagePack;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace DurableQueue
{
    public class DurableQueue<T> : IDisposable
    {
        private const int MAX_BUFFER_SIZE = 1_000_000;

        private readonly string _queueName;
        private readonly int _bufferSize;
        private readonly QueueDb _qDatabase;
        private readonly ConcurrentQueue<T> _queue = new();
        private readonly SemaphoreSlim _qSem = new(1);
        private readonly CancellationTokenSource _cts = new();
        private readonly Channel<T> _channel;

        private DurableQueue(string queueName, int bufferSize)
        {
            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException("Queue name cannot be null or empty");

            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException("Buffer size must be greater than zero");

            if (bufferSize > MAX_BUFFER_SIZE)
                throw new ArgumentOutOfRangeException($"Buffer size must be less than {MAX_BUFFER_SIZE}");

            _queueName = queueName;
            _bufferSize = bufferSize;
            _qDatabase = QueueDb.CreateQueue(queueName, _cts);

            _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(MAX_BUFFER_SIZE)
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
        }

        public void Dispose()
        {
            _cts.Cancel();
            _qDatabase.Dispose();
            _qSem.Release();
            _qSem.Dispose();
        }

        public int Count => _queue.Count;

        public string QueueName => _queueName;

        public async Task Enqueue(T item)
        {
            if (item == null)
                throw new ArgumentNullException("Item cannot be null");

            await _channel.Writer.WriteAsync(item);
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
            finally
            {
                _qSem.Release();
            }
        }

        public static async Task<DurableQueue<T>> CreateAsync(string queueName, int bufferSize = 100_000)
        {
            var queue = new DurableQueue<T>(queueName, bufferSize);
            await queue.LoadQueueFromDatabase();

            await Task.Factory.StartNew(queue.BufferEnqueueTask, queue._cts.Token,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return queue;
        }

        private async Task BufferEnqueueTask()
        {
            var buffer = new List<T>();

            while (!_cts.IsCancellationRequested)
            {
                while (await _channel.Reader.WaitToReadAsync(_cts.Token))
                {
                    buffer.Clear();

                    await foreach (var item in _channel.Reader.ReadAllAsync(_cts.Token))
                    {
                        if (item != null)
                        {
                            buffer.Add(item);
                        }

                        if (buffer.Count >= _bufferSize || _cts.IsCancellationRequested)
                        {
                            break;
                        }
                    }

                    await BulkEnqueue(buffer);
                }
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
            finally
            {
                _qSem.Release();
            }
        }

        private async Task LoadQueueFromDatabase()
        {
            try
            {
                await _qSem.WaitAsync(_cts.Token);

                await foreach (var item in _qDatabase.LoadItemsToMemory(_bufferSize))
                {
                    var deserializedItem = MessagePackSerializer.Deserialize<T>(item);
                    _queue.Enqueue(deserializedItem);
                }
            }
            finally
            {
                _qSem.Release();
            }
        }
    }
}

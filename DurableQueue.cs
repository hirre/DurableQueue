using DurableQueue.Repository;
using MessagePack;
using System.Collections.Concurrent;

namespace DurableQueue
{
    public class DurableQueue<T> : IDisposable
    {
        private readonly string _queueName;
        private QueueDb _qDb;
        private readonly ConcurrentQueue<T> _queue = new();
        private readonly SemaphoreSlim _qSem = new(1);

        public DurableQueue(string queueName)
        {
            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException("Queue name cannot be null or empty");

            _queueName = queueName;
            _qDb = QueueDb.CreateQueue(queueName);

            LoadQueueFromDatabase().GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            _qDb.Dispose();
            _qSem.Dispose();
        }

        private async Task LoadQueueFromDatabase()
        {
            try
            {
                await _qSem.WaitAsync();

                await foreach (var item in _qDb.LoadItemsToMemory())
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

        public async Task Enqueue(IEnumerable<T> items)
        {
            if (items == null || !items.Any())
                throw new ArgumentNullException("Item cannot be null");

            try
            {
                _qSem.Wait(5000);

                var byteList = new List<byte[]>();

                foreach (var item in items)
                {
                    var bytes = MessagePackSerializer.Serialize(item);
                    byteList.Add(bytes);
                }

                await _qDb.Enqueue(byteList);

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
                await _qSem.WaitAsync(5000);

                var retList = new List<T?>();

                while (_queue.Count > 0 && cnt != retList.Count)
                {
                    if (_queue.TryDequeue(out T? qItem))
                    {
                        retList.Add(qItem);
                    }
                }

                if (retList.Count != 0)
                    await _qDb.Delete(retList.Count);

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

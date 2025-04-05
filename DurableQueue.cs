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

        public async Task Enqueue(T item)
        {
            if (item == null)
                throw new ArgumentNullException("Item cannot be null");

            try
            {
                _qSem.Wait(5000);

                // Convert item to byte array
                var bytes = MessagePackSerializer.Serialize(item);
                await _qDb.Enqueue(bytes);
                _queue.Enqueue(item);
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

        public async Task<T?> Dequeue()
        {
            try
            {
                await _qSem.WaitAsync(5000);

                var item = MessagePackSerializer.Deserialize<T>(await _qDb.Dequeue());
                _queue.TryDequeue(out T? qItem);

                return qItem;
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

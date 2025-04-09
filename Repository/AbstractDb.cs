using DurableQueue.Interfaces;
using System.Collections.Concurrent;

namespace DurableQueue.Repository
{
    public abstract class AbstractDb : IDisposable
    {
        private readonly static ConcurrentDictionary<string, IRepository> _instances = new();

        public string QueueName { get; private set; }

        public CancellationTokenSource Cts { get; private set; }

        public AbstractDb(string queueName, CancellationTokenSource cts)
        {
            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException("Queue name cannot be null or empty");

            if (cts == null)
                throw new ArgumentNullException("Cancellation token source cannot be null");

            QueueName = queueName;
            Cts = cts;
        }

        public static T CreateQueue<T>(string queueName, CancellationTokenSource cts) where T : IRepository
        {
            if (_instances.ContainsKey(queueName))
            {
                return (T)_instances[queueName];
            }

            var instance = (T)Activator.CreateInstance(typeof(T), queueName, cts)!;
            _instances[queueName] = instance;

            return instance;
        }

        public virtual void Dispose()
        {
            _instances.TryRemove(QueueName, out _);
            GC.SuppressFinalize(this);
        }
    }
}

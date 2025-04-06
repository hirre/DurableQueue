using DurableQueue;

namespace DurableQueueTests
{
    public class QueueTests
    {
        // cleanup
        private const string TestQueueName = "testqueue";

        public QueueTests()
        {
            DelFile($"{TestQueueName}.db");
            DelFile($"{TestQueueName}.db-shm");
            DelFile($"{TestQueueName}.db-wal");
        }

        private void DelFile(string fileName)
        {
            var qd = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "QueueData");
            var file = Path.Combine(qd, fileName);

            if (File.Exists(file))
            {
                bool canAccess;

                // Check if we can access the file and that another process doesn't have it open
                do
                {
                    canAccess = false;

                    try
                    {
                        using var fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.None);
                        canAccess = true;
                    }
                    catch (IOException)
                    {
                        // File is locked by another process
                        Task.Delay(1000).Wait();
                    }

                } while (!canAccess);
                File.Delete(file);
            }
        }

        [Fact]
        public async Task Enqueue_Items()
        {
            await Task.Delay(5000); // Give time for cleanup

            using var queue = new DurableQueue<int>(TestQueueName);

            var itemCnt = 1000_000;

            for (int i = 0; i < itemCnt; i++)
            {
                queue.Enqueue(Random.Shared.Next());
            }

            await WaitAsync(() => queue.Count, itemCnt);

            if (queue.Count != itemCnt)
            {
                Assert.Fail($"Queue count {queue.Count} is not equal to {itemCnt} after 120 s");
            }
        }

        [Fact]
        public async Task Dequeue_Items()
        {
            await Task.Delay(5000); // Give time for cleanup

            // Arrange
            using var queue = new DurableQueue<int>(TestQueueName);
            var list = new List<int>();

            var itemCnt = 1000_000;

            for (int i = 0; i < itemCnt; i++)
            {
                queue.Enqueue(Random.Shared.Next());
            }

            await WaitAsync(() => queue.Count, itemCnt);

            var items = await queue.Dequeue(itemCnt);

            await WaitAsync(() => queue.Count, itemCnt, increasing: false);

            if (queue.Count != 0)
            {
                Assert.Fail($"Queue count {queue.Count} is not equal to 0 after 120 s");
            }
        }

        private async Task WaitAsync(Func<int> queueSize, int itemCnt, int delayMax = 120, bool increasing = true)
        {
            var delay = 0;

            if (increasing)
            {
                while (queueSize() < itemCnt && delay++ < delayMax)
                {
                    await Task.Delay(1000);
                }
            }
            else
            {
                while (queueSize() > itemCnt && delay++ < delayMax)
                {
                    await Task.Delay(1000);
                }
            }
        }
    }
}

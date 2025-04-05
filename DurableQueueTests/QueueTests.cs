using DurableQueue;

namespace DurableQueueTests
{
    public class QueueTests
    {
        // cleanup
        private const string TestQueueName = "testqueue";

        public QueueTests()
        {
            if (File.Exists(TestQueueName))
            {
                File.Delete(TestQueueName);
            }
        }

        [Fact]
        public async Task Enqueue_Items()
        {
            // Arrange
            var queue = new DurableQueue<int>(TestQueueName);
            var list = new List<int>();

            for (int i = 0; i < 1_000_000; i++)
            {
                list.Add(Random.Shared.Next());
            }

            await queue.BulkEnqueue(list);
        }

        [Fact]
        public async Task Dequeue_Items()
        {
            // Arrange
            var queue = new DurableQueue<int>(TestQueueName);
            var list = new List<int>();

            var itemCnt = 1000_000;

            for (int i = 0; i < itemCnt; i++)
            {
                list.Add(Random.Shared.Next());
            }

            await queue.BulkEnqueue(list);

            var item = await queue.BulkDequeue(itemCnt);
        }
    }
}

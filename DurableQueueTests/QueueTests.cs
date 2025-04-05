using DurableQueue;

namespace DurableQueueTests
{
    public class QueueTests
    {
        // cleanup
        private const string TestQueueName = "testqueue";

        ~QueueTests()
        {
            if (File.Exists(TestQueueName))
            {
                File.Delete(TestQueueName);
            }
        }

        [Fact]
        public async Task Enqueue_ShouldAddItemToQueue()
        {
            // Arrange
            var queue = new DurableQueue<int>(TestQueueName);

            for (int i = 0; i < 100_000; i++)
            {
                await queue.Enqueue(Random.Shared.Next());
            }
        }
    }
}

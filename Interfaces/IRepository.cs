namespace DurableQueue.Interfaces
{
    public interface IRepository : IDisposable
    {
        public IAsyncEnumerable<byte[]> LoadItemsToMemory(int limit);

        public Task Enqueue(IEnumerable<byte[]> items);

        public Task Delete(int nrOfItems);
    }
}

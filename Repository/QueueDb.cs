using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;

namespace DurableQueue.Repository
{
    internal class QueueDb : IDisposable
    {
        private SqliteConnection? _connection;
        private string _queueName;
        private static ConcurrentDictionary<string, QueueDb> _instances = new();

        internal static QueueDb CreateQueue(string queueName)
        {
            if (_instances.ContainsKey(queueName))
            {
                return _instances[queueName];
            }

            return new QueueDb(queueName);
        }

        private QueueDb(string queueName)
        {
            _queueName = queueName;

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException("Queue name cannot be null or empty");

            var folderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "QueueData");

            if (!Directory.Exists(folderPath))
            {
                Directory.CreateDirectory(folderPath);
            }

            var dataSource = Path.Combine(folderPath, $"{queueName}.db");
            _connection = new SqliteConnection($"Data Source={dataSource}");

            _connection.Open();

            using (var command = _connection.CreateCommand())
            {
                command.CommandText = @"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA cache_size = 100000;
                    PRAGMA temp_store = MEMORY;
                    PRAGMA locking_mode = EXCLUSIVE;
                ";

                command.ExecuteNonQuery();
            }

            CreateDbQueueIfNotExists().GetAwaiter().GetResult();

            _instances.TryAdd(_queueName, this);
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _instances.TryRemove(_queueName, out _);
        }

        private async Task CreateDbQueueIfNotExists()
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            var command = _connection.CreateCommand();

            command.CommandText =
            @"
                CREATE TABLE IF NOT EXISTS queue (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Item BLOB NOT NULL,
                    CreatedAt TEXT NOT NULL
                );
            ";

            await command.ExecuteNonQueryAsync();
        }

        internal async IAsyncEnumerable<byte[]> LoadItemsToMemory()
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            var command = _connection.CreateCommand();

            command.CommandText =
            @"
                SELECT Item
                FROM queue
                ORDER BY Id ASC;
            ";

            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var item = (byte[])reader["Item"];
                yield return item;
            }
        }

        internal async Task Enqueue(IEnumerable<byte[]> items)
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            using var transaction = _connection.BeginTransaction();

            try
            {
                var command = _connection.CreateCommand();

                command.Transaction = transaction;
                command.CommandText = "INSERT INTO queue (Item, CreatedAt) VALUES (@item, @createdAt)";
                var itemParam = new SqliteParameter("@item", SqliteType.Blob);
                var dateTimeParam = new SqliteParameter("@createdAt", SqliteType.Text);

                command.Parameters.Add(itemParam);
                command.Parameters.Add(dateTimeParam);

                foreach (var data in items)
                {
                    itemParam.Value = data;
                    dateTimeParam.Value = DateTime.UtcNow.ToString("o");
                    await command.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                throw new InvalidOperationException($"Failed to enqueue item: {ex.Message}", ex);
            }
        }

        internal async Task Delete(int nrOfItems)
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            using var transaction = _connection.BeginTransaction();

            try
            {

                var sqlDelNrParam = new SqliteParameter("@delNr", SqliteType.Integer)
                {
                    Value = nrOfItems
                };

                // Remove the dequeued item from the queue
                var deleteCommand = _connection.CreateCommand();
                deleteCommand.Transaction = transaction;
                deleteCommand.CommandText = "DELETE FROM queue WHERE Id IN (SELECT Id FROM queue ORDER BY Id ASC LIMIT @delNr)";
                deleteCommand.Parameters.Add(sqlDelNrParam);
                await deleteCommand.ExecuteNonQueryAsync();

                await transaction.CommitAsync();

            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                throw new InvalidOperationException($"Failed to dequeue item: {ex.Message}", ex);
            }
        }
    }
}

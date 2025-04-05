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

            _connection = new SqliteConnection($"Data Source={queueName}.db");
            _connection.Open();

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

        internal async Task Enqueue(byte[] item)
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            using var transaction = _connection.BeginTransaction();

            try
            {
                var command = _connection.CreateCommand();

                command.CommandText =
                @"
                    INSERT INTO queue (Item, CreatedAt)
                    VALUES ($item, $createdAt);
                ";

                command.Parameters.AddWithValue("$item", item);
                command.Parameters.AddWithValue("$createdAt", DateTime.UtcNow.ToString("o"));

                await command.ExecuteNonQueryAsync();
                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                throw new InvalidOperationException($"Failed to enqueue item: {ex.Message}", ex);
            }
        }

        internal async Task<byte[]?> Dequeue()
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            using var transaction = _connection.BeginTransaction();

            try
            {
                var command = _connection.CreateCommand();

                command.CommandText =
                @"
                    SELECT Id, Item
                    FROM queue
                    ORDER BY Id ASC
                    LIMIT 1;
                ";

                using var reader = await command.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    var id = (long)reader["Id"];
                    var item = (byte[])reader["Item"];

                    // Remove the dequeued item from the queue
                    var deleteCommand = _connection.CreateCommand();
                    deleteCommand.Transaction = transaction;
                    deleteCommand.CommandText = "DELETE FROM queue WHERE Id = $id";
                    deleteCommand.Parameters.AddWithValue("$id", id);
                    await deleteCommand.ExecuteNonQueryAsync();

                    await transaction.CommitAsync();

                    return item;
                }
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                throw new InvalidOperationException($"Failed to dequeue item: {ex.Message}", ex);
            }

            return null;
        }
    }
}

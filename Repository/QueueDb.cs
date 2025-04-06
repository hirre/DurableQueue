using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;

namespace DurableQueue.Repository
{
    internal class QueueDb : IDisposable
    {
        private SqliteConnection? _connection;
        private string _queueName;
        private static ConcurrentDictionary<string, QueueDb> _instances = new();
        private CancellationTokenSource _cts;

        internal static QueueDb CreateQueue(string queueName, CancellationTokenSource cts)
        {
            if (_instances.ContainsKey(queueName))
            {
                return _instances[queueName];
            }

            return new QueueDb(queueName, cts);
        }

        private QueueDb(string queueName, CancellationTokenSource cts)
        {
            _queueName = queueName;
            _cts = cts;

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
            _connection?.Close();
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

            await command.ExecuteNonQueryAsync(_cts.Token);
        }

        internal async IAsyncEnumerable<byte[]> LoadItemsToMemory(int limit = 100000)
        {
            if (_connection == null)
                throw new InvalidOperationException("Database connection is not initialized.");

            var command = _connection.CreateCommand();
            var offset = 0;
            bool hasValues;

            command.CommandText = @"
                    SELECT Item
                    FROM queue
                    ORDER BY Id ASC
                    LIMIT @limit OFFSET @offset;
                ";

            var limitParam = new SqliteParameter("@limit", SqliteType.Integer);
            var offsetParam = new SqliteParameter("@offset", SqliteType.Integer);
            command.Parameters.Add(limitParam);
            command.Parameters.Add(offsetParam);

            do
            {
                hasValues = false;

                limitParam.Value = limit;
                offsetParam.Value = offset;

                using var reader = await command.ExecuteReaderAsync(_cts.Token);

                while (await reader.ReadAsync(_cts.Token))
                {
                    hasValues = true;
                    var item = (byte[])reader["Item"];
                    yield return item;
                }

                offset += limit;

            } while (hasValues);
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
                    await command.ExecuteNonQueryAsync(_cts.Token);
                }

                await transaction.CommitAsync(_cts.Token);
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync(_cts.Token);
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
                await deleteCommand.ExecuteNonQueryAsync(_cts.Token);

                await transaction.CommitAsync(_cts.Token);

            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync(_cts.Token);
                throw new InvalidOperationException($"Failed to dequeue item: {ex.Message}", ex);
            }
        }
    }
}

using DurableQueue.Interfaces;
using Microsoft.Data.Sqlite;
using System.Runtime.ExceptionServices;

namespace DurableQueue.Repository
{
    public class SqliteQueueDbImpl : AbstractDb, IDisposable, IRepository
    {
        private readonly SqliteConnection? _connection;

        public SqliteQueueDbImpl(string queueName, CancellationTokenSource cts) : base(queueName, cts)
        {
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
        }

        public override void Dispose()
        {
            _connection?.Close();
            _connection?.Dispose();
            base.Dispose();
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

            await command.ExecuteNonQueryAsync(Cts.Token);
        }

        public async IAsyncEnumerable<byte[]> LoadItemsToMemory(int limit)
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

                using var reader = await command.ExecuteReaderAsync(Cts.Token);

                while (await reader.ReadAsync(Cts.Token))
                {
                    hasValues = true;
                    var item = (byte[])reader["Item"];
                    yield return item;
                }

                offset += limit;

            } while (hasValues);
        }

        public async Task Enqueue(IEnumerable<byte[]> items)
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
                    await command.ExecuteNonQueryAsync(Cts.Token);
                }

                await transaction.CommitAsync(Cts.Token);
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync(Cts.Token);
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }

        public async Task Delete(int nrOfItems)
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
                await deleteCommand.ExecuteNonQueryAsync(Cts.Token);

                await transaction.CommitAsync(Cts.Token);

            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync(Cts.Token);
                ExceptionDispatchInfo.Capture(ex).Throw();
            }
        }
    }
}

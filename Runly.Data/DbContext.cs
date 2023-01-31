using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Runly.Data
{
    public class DbContext : DbConnection
    {
        public DbConnection InnerConnection { get; }
        public DbTransaction? CurrentTransaction { get; private set; }

        public DbContext(DbConnection inner)
        {
            InnerConnection = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
                Open();

            CurrentTransaction = InnerConnection.BeginTransaction(isolationLevel);
            return CurrentTransaction;
        }

        protected override DbCommand CreateDbCommand()
        {
            var cmd = InnerConnection.CreateCommand();
            cmd.Transaction = CurrentTransaction;
            return cmd;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                InnerConnection?.Dispose();
            }
        }

        [AllowNull]
        public override string ConnectionString
        {
            get => InnerConnection.ConnectionString;
            set => InnerConnection.ConnectionString = value;
        }

        public override event StateChangeEventHandler? StateChange
        {
            add { InnerConnection.StateChange += value; }
            remove { InnerConnection.StateChange -= value; }
        }

        public override string Database => InnerConnection.Database;
        public override string DataSource => InnerConnection.DataSource;
        public override string ServerVersion => InnerConnection.ServerVersion;
        public override ConnectionState State => InnerConnection.State;
        public override int ConnectionTimeout => InnerConnection.ConnectionTimeout;

        public override void ChangeDatabase(string databaseName) => InnerConnection.ChangeDatabase(databaseName);

        public override void Open() => InnerConnection.Open();
        public override Task OpenAsync(CancellationToken cancellationToken) => InnerConnection.OpenAsync(cancellationToken);
        public override void Close() => InnerConnection.Close();
    }
}

using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;

namespace Runly.Data.Sql
{
    public class Database
    {
        const int azureGracePeriod = 3000;
        static readonly Regex goEx = new Regex(@"^\s*go\s*$", RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

        readonly string connectionString;
        readonly int? commandTimeout;

        readonly int schemaVersion;
        readonly string[] createSql;
        readonly string[] clearSql;
        readonly string[] populateSql;

        readonly ILogger<Database> logger;

        public string ConnectionString => connectionString;

        /// <summary>
        /// Initializes a new <see cref="Database"/> using the <paramref name="createScript"/>, <paramref name="clearScript"/>, and <paramref name="populateScript"/> provided.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="schemaVersion"></param>
        /// <param name="createScript"></param>
        /// <param name="clearScript"></param>
        /// <param name="populateScript"></param>
        /// <param name="logger"></param>
        /// <param name="commandTimeout"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public Database(string connectionString, int schemaVersion, string createScript, string clearScript, string populateScript, ILogger<Database> logger, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            if (string.IsNullOrEmpty(createScript))
                throw new ArgumentNullException(nameof(createScript));

            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
            this.logger = logger;

            this.schemaVersion = schemaVersion;
            createSql = ParseScript(createScript);
            clearSql = ParseScript(clearScript ?? string.Empty);
            populateSql = ParseScript(populateScript ?? string.Empty);
        }

        /// <summary>
        /// Initializes a <see cref="Database"/> using SQL scripts included as embedded resources found in the assembly and namespace of the <see cref="Type"/> <paramref name="namespace"/>. Expected script names are create.sql, clear.sql, and populate.sql.
        /// </summary>
        public Database(string connectionString, int schemaVersion, Type @namespace, ILogger<Database> logger, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            if (@namespace == null)
                throw new ArgumentNullException(nameof(@namespace));

            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
            this.logger = logger;

            this.schemaVersion = schemaVersion;
            createSql = ParseScript(ReadScriptFromEmbeddedResource(@namespace, "create"));
            clearSql = ParseScript(ReadScriptFromEmbeddedResource(@namespace, "clear"));
            populateSql = ParseScript(ReadScriptFromEmbeddedResource(@namespace, "populate"));
        }

        string ReadScriptFromEmbeddedResource(Type @namespace, string name)
        {
            string resourceName = $"{@namespace.Namespace}.{name}.sql";

            using (var str = @namespace.Assembly.GetManifestResourceStream(resourceName))
            {
                if (str == null)
                {
                    string availableResources = @namespace.Assembly.GetManifestResourceNames().Aggregate(new StringBuilder(), (sb, r) => sb.AppendLine(r)).ToString();
                    throw new ArgumentException($"Could not find embedded resource '{resourceName}'. Available resources in assembly: {availableResources}");
                }

                using (var reader = new StreamReader(str))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        string[] ParseScript(string script)
        {
            return goEx.Split(script).Where(c => !goEx.IsMatch(c) && !string.IsNullOrWhiteSpace(c)).ToArray();
        }

        public async Task MigrateAsync()
        {
            if (!await ExistsAsync())
            {
                // db doesn't exist; build it
                await BuildAsync();
                return;
            }

            int dbVersion = await GetSchemaVersionAsync();
            if (dbVersion == schemaVersion)
            {
                logger.LogInformation($"Database schema version {dbVersion} is current.");
                return; // all good; don't do anything
            }
            else
            {
                logger.LogInformation($"Database schema version {dbVersion} does is not current ({schemaVersion}).");
            }

            // need to blow the database away
            await RebuildAsync();
        }

        async Task<int> GetSchemaVersionAsync()
        {
            int version = 0;

            using (var conn = new SqlConnection(connectionString))
            {
                try
                {
                    version = await conn.ExecuteScalarAsync<int>("select top 1 [version] from dbo.[dbschema] order by version desc;", commandTimeout: commandTimeout);
                }
                catch (SqlException) { /* if table doesn't exist, don't blow up, just return no version */ }
            }

            return version;
        }

        async Task<bool> ExistsAsync()
        {
            var builder = new SqlConnectionStringBuilder(connectionString);

            string database = builder.InitialCatalog;
            builder.InitialCatalog = "";

            using (var conn = new SqlConnection(builder.ToString()))
            {
                bool exists = await conn.ExecuteScalarAsync<bool>("SELECT TOP 1 1 FROM sys.sysdatabases WHERE [Name] = @database", new { database }, commandTimeout: commandTimeout);

                logger.LogInformation($"Database '{database}'{(exists ? " exists" : " does not exist")}.");

                return exists;
            }
        }

        public async Task RebuildAsync()
        {
            await DropAsync();
            await BuildAsync();
        }

        public async Task BuildAsync()
        {
            var builder = new SqlConnectionStringBuilder(connectionString);

            string database = builder.InitialCatalog;
            builder.InitialCatalog = "";

            bool isAzure = false;
            using (var conn = new SqlConnection(builder.ToString()))
            {
                isAzure = await IsAzure(conn);

                logger.LogInformation($"Building database '{database}'{(isAzure ? " on azure" : "")}...");

                if (isAzure)
                {
                    await conn.ExecuteAsync($"IF NOT EXISTS (SELECT 1 FROM sys.sysdatabases WHERE name = '{database}') CREATE DATABASE [{database}] ( EDITION = 'basic')", commandTimeout: commandTimeout);
                }
                else
                {
                    await conn.ExecuteAsync($"IF NOT EXISTS (SELECT 1 FROM sys.sysdatabases WHERE name = '{database}') CREATE DATABASE [{database}]", commandTimeout: commandTimeout);
                }
            }

            if (isAzure)
                await Task.Delay(azureGracePeriod);

            using (var conn = new SqlConnection(connectionString))
            {
                if (isAzure)
                {
                    logger.LogInformation($"Creating Azure logins/roles...");

                    await conn.ExecuteAsync("CREATE USER [core-prod] FROM LOGIN [core-prod]");
                    await conn.ExecuteAsync("EXEC sp_addrolemember 'db_owner', 'core-prod'");
                }

                await BuildSchemaAsync(conn);
            }
        }

        public async Task BuildSchemaAsync(DbConnection conn)
        {
            await conn.EnsureOpenAsync();

            logger.LogInformation($"Creating database schema...");

            foreach (string statement in createSql)
                await ExecuteScriptAsync(conn, statement);

            logger.LogInformation($"Populating database...");

            foreach (string statement in populateSql)
                await ExecuteScriptAsync(conn, statement);

            await conn.ExecuteAsync("insert into dbo.[dbschema] ([version]) values (@version)", new { schemaVersion });
        }

        public async Task ClearAsync()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                await ClearAsync(conn);
            }
        }

        public async Task ClearAsync(DbConnection conn)
        {
            await conn.EnsureOpenAsync();

            logger.LogInformation($"Clearing database...");

            using (var tx = conn.BeginTransaction())
            {
                foreach (string statement in clearSql)
                    await ExecuteScriptAsync(conn, statement, tx);

                tx.Commit();
            }
        }

        public async Task DropAsync()
        {
            var builder = new SqlConnectionStringBuilder(connectionString);

            string database = builder.InitialCatalog;
            builder.InitialCatalog = "";

            bool isAzure;
            using (var conn = new SqlConnection(builder.ToString()))
            {
                isAzure = await IsAzure(conn);

                logger.LogInformation($"Dropping database '{database}'{(isAzure ? " on azure" : "")}...");

                if (isAzure)
                {
                    await conn.ExecuteAsync($@"DROP DATABASE IF EXISTS [{database}]", commandTimeout: commandTimeout);
                }
                else
                {
                    await conn.ExecuteAsync($@"
						IF EXISTS (SELECT 1 FROM sys.sysdatabases WHERE name = '{database}')
						BEGIN
							ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
							DROP DATABASE [{database}];
						END", commandTimeout: commandTimeout);
                }
            }

            if (isAzure)
                await Task.Delay(azureGracePeriod);
        }

        async Task ExecuteScriptAsync(DbConnection conn, string sql, IDbTransaction tx = null)
        {
            try
            {
                await conn.ExecuteAsync(sql, transaction: tx, commandTimeout: commandTimeout);
            }
            catch (SqlException ex)
            {
                throw new SqlStatementException(ex, sql);
            }
        }

        Task<bool> IsAzure(DbConnection conn) => conn.ExecuteScalarAsync<bool>("SELECT CASE WHEN SERVERPROPERTY ('edition') = N'SQL Azure' THEN 1 END", commandTimeout: commandTimeout);

    }
}

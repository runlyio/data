using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.RegularExpressions;

namespace Runly.Data.Sql
{
    public class DatabaseBuilder
    {
        const int azureGracePeriod = 3000;
        static readonly Regex goEx = new Regex(@"^\s*go\s*$", RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

        readonly string connectionString;
        readonly int? commandTimeout;

        readonly string[] createSql;
        readonly string[] clearSql;
        readonly string[] populateSql;

        readonly ILogger<DatabaseBuilder> logger;

        public string ConnectionString => connectionString;

        /// <summary>
        /// Initializes a new <see cref="DatabaseBuilder"/> using the <paramref name="createScript"/>, <paramref name="clearScript"/>, and <paramref name="populateScript"/> provided.
        /// </summary>
        public DatabaseBuilder(string connectionString, string createScript, string clearScript, string populateScript, ILogger<DatabaseBuilder> logger, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            if (string.IsNullOrEmpty(createScript))
                throw new ArgumentNullException(nameof(createScript));

            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
            this.logger = logger;

            createSql = ParseScript(createScript);
            clearSql = ParseScript(clearScript ?? string.Empty);
            populateSql = ParseScript(populateScript ?? string.Empty);
        }

        /// <summary>
        /// Initializes a <see cref="DatabaseBuilder"/> using SQL scripts included as embedded resources found in the assembly and namespace of the <see cref="Type"/> <paramref name="namespace"/>. Expected script names are create.sql, clear.sql, and populate.sql.
        /// </summary>
        public DatabaseBuilder(string connectionString, Type @namespace, ILogger<DatabaseBuilder> logger, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            if (@namespace == null)
                throw new ArgumentNullException(nameof(@namespace));

            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
            this.logger = logger;

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

        async Task ExecuteScriptAsync(DbConnection conn, string sql, IDbTransaction? tx = null)
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

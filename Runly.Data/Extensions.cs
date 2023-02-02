using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runly.Data.Sql;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace Runly.Data
{
	public static class Extensions
	{
		public static async Task EnsureOpenAsync(this DbConnection conn)
		{
			if (conn.State != ConnectionState.Open)
				await conn.OpenAsync();
		}

		/// <summary>
		/// Adds MSSQL support for the database found at the <paramref name="connectionString"/> supplied.
		/// </summary>
		/// <param name="connectionString">Connection string to the database.</param>
		/// <param name="opts">Options for database column name mapping.</param>
		/// <returns></returns>
		public static IServiceCollection AddDatabase(this IServiceCollection services, string connectionString, DatabaseOptions? opts = default)
		{
			opts = opts ?? new DatabaseOptions();

			services.AddScoped<IDbConnection>(s => new DbContext(new SqlConnection(connectionString)));

			DefaultTypeMap.MatchNamesWithUnderscores = opts.MatchColumnNamesWithUnderscores;

			foreach (var remove in opts.ColumnPrefixesToRemove)
			{
				SqlMapper.SetTypeMap(remove.Key, new CustomPropertyTypeMap(remove.Key, (t, c) =>
				{
					if (c.StartsWith(remove.Value, StringComparison.InvariantCultureIgnoreCase))
						c = c.Remove(0, remove.Value.Length);

					if (opts.MatchColumnNamesWithUnderscores)
						c = c.Replace("_", "");

					return t.GetProperty(c, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
				}));
			}

			return services;
		}

		/// <summary>
		/// Add support for building, rebuilding, and clearing a database using SQL scripts included as embedded resources found in the namespace of the type <paramref name="scriptsNamespace"/>.
		/// </summary>
		/// <param name="connectionString">Connection string to the database.</param>
		/// <param name="scriptsNamespace">A type found in the namespace of the SQL scripts included as embedded resources. The required file names are create.sql, populate.sql, and clear.sql.</param>
		/// <returns></returns>
		public static IServiceCollection AddDatabaseBuilder(this IServiceCollection services, string connectionString, Type scriptsNamespace)
		{
			services.AddScoped(s => new DatabaseBuilder(connectionString, scriptsNamespace, s.GetRequiredService<ILogger<DatabaseBuilder>>()));

			return services;
		}
	}
}
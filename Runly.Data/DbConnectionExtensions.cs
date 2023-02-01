using System.Data;
using System.Data.Common;

namespace Runly.Data
{
	public static class DbConnectionExtensions
	{
		public static async Task EnsureOpenAsync(this DbConnection conn)
		{
			if (conn.State != ConnectionState.Open)
				await conn.OpenAsync();
		}
	}
}
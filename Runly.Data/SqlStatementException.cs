namespace Runly.Data
{
    public class SqlStatementException : Exception
    {
        readonly string sql;

        public SqlStatementException(Exception ex, string sql)
            : base("Error executing: " + sql, ex)
        {
            this.sql = sql;
        }
    }
}

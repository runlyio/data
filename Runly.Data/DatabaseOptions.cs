namespace Runly.Data
{
    public class DatabaseOptions
    {
        public bool MatchColumnNamesWithUnderscores { get; set; } = true;
        public Dictionary<Type, string> ColumnPrefixesToRemove { get; } = new Dictionary<Type, string>();
    }
}

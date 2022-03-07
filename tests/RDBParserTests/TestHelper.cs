namespace RDBParserTests
{
    public static class TestHelper
    {
        public static string GetRDBPath(string fileName)
        {
            var dir = System.AppContext.BaseDirectory;
            var path = System.IO.Path.Combine(dir, "dictionary.rdb");
            return path;
        }    
    }
}
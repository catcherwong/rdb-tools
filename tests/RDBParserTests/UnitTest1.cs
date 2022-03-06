using RDBParser;
using Xunit;

namespace RDBParserTests
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            var dir = System.AppContext.BaseDirectory;
            var path = System.IO.Path.Combine(dir, "dictionary.rdb");

            var parser = new DefaultRDBParser(new DefaultConsoleReaderCallBack());
            parser.Parse(path);

            Assert.True(true);
        }
    }
}
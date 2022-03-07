using RDBParser;
using Xunit;

namespace RDBParserTests
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            var path = TestHelper.GetRDBPath("dictionary.rdb");

            var parser = new BinaryReaderRDBParser(new DefaultConsoleReaderCallBack());
            parser.Parse(path);

            Assert.True(true);
        }
    }
}
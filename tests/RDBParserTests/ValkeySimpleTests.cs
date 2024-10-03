using RDBParser;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class ValkeySimpleTests
    {
        private ITestOutputHelper _output;

        public ValkeySimpleTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestIsValkey()
        {
            // set mykey v1
            // bgsave
            var path = TestHelper.GetRDBPath("valkey_80_normal.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);


            Assert.True(parser.IsValkey());
        }
    }

}
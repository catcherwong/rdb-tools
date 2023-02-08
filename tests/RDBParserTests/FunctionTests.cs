using RDBParser;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class FunctionTests
    {
        private ITestOutputHelper _output;

        public FunctionTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestFunctionWithRedis70()
        {
            // FUNCTION LOAD "#!lua name=mylib\nredis.register_function('knockknock', function() return 'Who\\'s there?' end)"
            // FUNCTION LOAD "#!lua name=mylib2\nredis.register_function('knockknock2', function() return 'Who\\'s there?' end)"
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70_with_function.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var func = callback.GetFunctions();

            Assert.True(func.ContainsKey(Encoding.UTF8.GetBytes("lua")));
            Assert.Equal(2, func[Encoding.UTF8.GetBytes("lua")].Count);
            Assert.Contains(Encoding.UTF8.GetBytes("mylib"), func[Encoding.UTF8.GetBytes("lua")]);
            Assert.Contains(Encoding.UTF8.GetBytes("mylib2"), func[Encoding.UTF8.GetBytes("lua")]);
        }
    }
}
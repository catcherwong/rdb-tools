using RDBParser;
using System.Text;
using Xunit;

namespace RDBParserTests
{
    public partial class ParserTests
    {
        [Fact]
        public void TestFunctionWithRedis70RC3()
        {
            // FUNCTION LOAD "#!lua name=mylib\nredis.register_function('knockknock', function() return 'Who\\'s there?' end)"
            // FUNCTION LOAD "#!lua name=mylib2\nredis.register_function('knockknock2', function() return 'Who\\'s there?' end)"
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70rc3_with_function.rdb");

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
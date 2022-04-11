using RDBParser;
using System.Text;
using Xunit;

namespace RDBParserTests
{
    public partial class ParserTests
    {
        [Fact]
        public void TestListWithRedis70RC3()
        {
            // lpush mylist abc
            // lpush mylist 123
            // lpush mylist efg
            // lpush mylist 99999999
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70rc3_with_list.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var sets = callback.GetSets();

            Assert.Equal(4, lengths[0][Encoding.UTF8.GetBytes("mylist")]);

            Assert.Contains(Encoding.UTF8.GetBytes("abc"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("efg"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(RedisRdbObjectHelper.LpConvertInt64ToBytes(123), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(RedisRdbObjectHelper.LpConvertInt64ToBytes(99999999), sets[0][Encoding.UTF8.GetBytes("mylist")]);
        }
    }
}
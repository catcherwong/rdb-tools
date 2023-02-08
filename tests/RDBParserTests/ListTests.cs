using RDBParser;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class ListTests
    {
        private ITestOutputHelper _output;

        public ListTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestListWithRedis70ListPack()
        {
            // lpush mylist abc
            // lpush mylist 202302071440
            // lpush mylist 0
            // lpush mylist 128
            // lpush mylist -128
            // lpush mylist 1234566
            // lpush mylist 1234566777
            // lpush mylist 2.6
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70_with_list.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var sets = callback.GetSets();

            Assert.Equal(8, lengths[0][Encoding.UTF8.GetBytes("mylist")]);

            Assert.Contains(Encoding.UTF8.GetBytes("abc"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("202302071440"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("0"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("128"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("-128"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("1234566"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("1234566777"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("2.6"), sets[0][Encoding.UTF8.GetBytes("mylist")]);
        }
    }
}
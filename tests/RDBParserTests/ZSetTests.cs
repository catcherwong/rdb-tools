using RDBParser;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class ZSetTests
    {
        private ITestOutputHelper _output;

        public ZSetTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestSortedSetAsZipList()
        {
            var path = TestHelper.GetRDBPath("sorted_set_as_ziplist.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sortedSets = callback.GetSortedSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("sorted_set_as_ziplist")]);
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("sorted_set_as_ziplist")][Encoding.UTF8.GetBytes("8b6ba6718a786daefa69438148361901")], 1));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("sorted_set_as_ziplist")][Encoding.UTF8.GetBytes("cb7a24bb7528f934b841b34c3a73e0c7")], 2.37f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("sorted_set_as_ziplist")][Encoding.UTF8.GetBytes("523af537946b79c4f8369ed39ba78605")], 3.423f));
        }

        [Fact]
        public void TestZSetWithRedis70ListPack()
        {
            // zadd myzset 1.1 one
            // zadd myzset 999.9 202302071440
            // zadd myzset -100.8 0
            // zadd myzset 12800 128
            // zadd myzset 1234566 -128
            // zadd myzset 900909090 1234566
            // zadd myzset -900909090 1234566777
            // zadd myzset 1234566777 2.6
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70_with_zset_listpack.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var sortedSets = callback.GetSortedSets();

            Assert.Equal(8, lengths[0][Encoding.UTF8.GetBytes("myzset")]);

            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("one")], 1.1f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("202302071440")], 999.9f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("0")], -100.8f));
            Assert.True(TestHelper.FloatEqueal((int)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("128")], 12800));
            Assert.True(TestHelper.FloatEqueal((int)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("-128")], 1234566));
            Assert.True(TestHelper.FloatEqueal((int)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("1234566")], 900909090));
            Assert.True(TestHelper.FloatEqueal((int)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("1234566777")], -900909090));
            Assert.True(TestHelper.FloatEqueal((int)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("2.6")], 1234566777));
        }
    }
}
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
        public void TestZSetWithRedis70RC3()
        {
            // zadd myzset 1.1 one
            // zadd myzset 999.9 two
            // zadd myzset -100.8 three
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70rc3_with_zset_listpack.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var sortedSets = callback.GetSortedSets();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("myzset")]);

            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("one")], 1.1f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("two")], 999.9f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("three")], -100.8f));
        }

        [Fact]
        public void TestZSetWithRedis70RC3AndInteger()
        {
            // zadd myzset 100 one
            // zadd myzset -100 two
            // zadd myzset 999999999 three
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70rc3_with_zset_listpack_and_integer.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var sortedSets = callback.GetSortedSets();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("myzset")]);

            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("one")], 100f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("two")], -100f));
            Assert.True(TestHelper.FloatEqueal((float)sortedSets[0][Encoding.UTF8.GetBytes("myzset")][Encoding.UTF8.GetBytes("three")], 999999999f));
        }
    }
}
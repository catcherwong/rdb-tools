using RDBParser;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class StreamTests
    {
        private ITestOutputHelper _output;

        public StreamTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestStreamsWithRedis50()
        {
            var path = TestHelper.GetRDBPath("redis_50_with_streams.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();

            Assert.Equal(4, lengths[0][Encoding.UTF8.GetBytes("mystream")]);
        }

        [Fact]
        public void TestStreamsWithRedis62AndMultipleDatabase()
        {
            // xadd mystream 1526919030474-55 message hello
            // xadd mystream 1526919030474-56 message world
            // xadd mystream 1649430004143-0 message abc
            // select 2
            // xadd mystr 1649430030198-0 f1 v1
            var path = TestHelper.GetRDBPath("redis_62_multiple_database_with_streams.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var streamEntities = callback.GetStreamEntities();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("mystream")]);
            Assert.Equal(1, lengths[2][Encoding.UTF8.GetBytes("mystr")]);

            var streamEntity0 = streamEntities[0][Encoding.UTF8.GetBytes("mystream")];
            var se0 = streamEntity0.Single();
            Assert.Equal("1649430004143-0", se0.LastId);
            Assert.Empty(se0.CGroups);

            var streamEntity2 = streamEntities[2][Encoding.UTF8.GetBytes("mystr")];
            var se2= streamEntity2.Single();
            Assert.Equal("1649430030198-0", se2.LastId);
            Assert.Empty(se2.CGroups);
        }

        [Fact]
        public void TestStreamsWithRedis62AndGroup()
        {
            // xadd mystream 1526919030474-55 message 1
            // xadd mystream 1526919030474-56 message 2
            // xadd mystream 1526919030474-57 message 3
            // XGROUP create mystream sg 0-0
            // XREADGROUP group sg c1 count 1 streams mystream >
            // XACK mystream sg "1526919030474-55"
            // XREADGROUP group sg c1 count 1 streams mystream >
            // bgsave
            var path = TestHelper.GetRDBPath("redis_62_with_streams_and_group.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var streamEntities = callback.GetStreamEntities();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("mystream")]);

            var streamEntity = streamEntities[0][Encoding.UTF8.GetBytes("mystream")];
            Assert.Single(streamEntity);

            var se = streamEntity.Single();
            
            var sgList = se.CGroups;
            Assert.Single(sgList);

            var sg = sgList.Single();
            Assert.Equal(Encoding.UTF8.GetBytes("sg"), sg.Name);
            Assert.Equal("1526919030474-56", sg.LastEntryId);
            Assert.Single(sg.Consumers);
            Assert.Single(sg.Pending);

            var consumer = sg.Consumers.Single();
            Assert.Equal(Encoding.UTF8.GetBytes("c1"), consumer.Name);
            Assert.Single(consumer.Pending);

            var pending = sg.Pending.Single();
            var streamId = RedisRdbObjectHelper.GetStreamId(pending.Id);
            Assert.Equal("1526919030474-56", streamId);
        }

        [Fact]
        public void TestStreamsWithRedis70AndGroup()
        {
            // xadd mystream 1526919030474-55 message 1
            // xadd mystream 1526919030474-56 message 2
            // xadd mystream 1526919030474-57 message 3
            // XGROUP create mystream sg 0-0
            // XREADGROUP group sg c1 count 1 streams mystream >
            // XACK mystream sg "1526919030474-55"
            // XREADGROUP group sg c1 count 1 streams mystream >
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70_with_streams_and_group.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var streamEntities = callback.GetStreamEntities();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("mystream")]);

            var streamEntity = streamEntities[0][Encoding.UTF8.GetBytes("mystream")];
            Assert.Single(streamEntity);

            var se = streamEntity.Single();

            Assert.Equal("1526919030474-55", se.FirstId);
            Assert.Equal("1526919030474-57", se.LastId);

            var sgList = se.CGroups;
            Assert.Single(sgList);

            var sg = sgList.Single();
            Assert.Equal(Encoding.UTF8.GetBytes("sg"), sg.Name);
            Assert.Equal("1526919030474-56", sg.LastEntryId);
            Assert.Single(sg.Consumers);
            Assert.Single(sg.Pending);
            Assert.Equal((ulong)2, sg.EntriesRead);

            var consumer = sg.Consumers.Single();
            Assert.Equal(Encoding.UTF8.GetBytes("c1"), consumer.Name);
            Assert.Single(consumer.Pending);

            var pending = sg.Pending.Single();
            var streamId = RedisRdbObjectHelper.GetStreamId(pending.Id);
            Assert.Equal("1526919030474-56", streamId);
        }
    }
}
using RDBParser;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class IdleAndFreqTests
    {
        private ITestOutputHelper _output;

        public IdleAndFreqTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestDisableIdleAndFreq()
        {
            // config get maxmemory-policy -> noeviction
            // set e1 v1 ex 6000
            // bgsave
            var path = TestHelper.GetRDBPath("noeviction.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var val = callback.GetIdleOrFreq();
            var infos = callback.GetInfos();

            Assert.Equal(-99, val);

            Assert.Equal(0, infos[0][Encoding.UTF8.GetBytes("e1")].Freq);
            Assert.Equal(0, (int)infos[0][Encoding.UTF8.GetBytes("e1")].Idle);
        }

        [Fact]
        public void TestEnableIdle()
        {
            // config set maxmemory-policy volatile-lru
            // set e1 v1 ex 6000
            // get e1
            // bgsave
            // bgsave
            var path = TestHelper.GetRDBPath("lru.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var val = callback.GetIdleOrFreq();
            var infos = callback.GetInfos();

            Assert.Equal(1, val);

            Assert.Equal(0, infos[0][Encoding.UTF8.GetBytes("e1")].Freq);
            Assert.Equal(9, (int)infos[0][Encoding.UTF8.GetBytes("e1")].Idle);

            var path2 = TestHelper.GetRDBPath("lru2.rdb");

            var callback2 = new TestReaderCallback(_output);
            var parser2 = new BinaryReaderRDBParser(callback2);
            parser2.Parse(path2);

            var val2 = callback2.GetIdleOrFreq();
            var infos2 = callback2.GetInfos();

            Assert.Equal(1, val2);

            Assert.Equal(0, infos2[0][Encoding.UTF8.GetBytes("e1")].Freq);
            Assert.Equal(32, (int)infos2[0][Encoding.UTF8.GetBytes("e1")].Idle);
        }

        [Fact]
        public void TestEnableFreq()
        {
            // config set maxmemory-policy volatile-lfu
            // set e1 v1 ex 6000
            // get e1
            // bgsave
            // bgsave
            var path = TestHelper.GetRDBPath("lfu.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var val = callback.GetIdleOrFreq();
            var infos = callback.GetInfos();

            Assert.Equal(2, val);

            Assert.Equal(6, infos[0][Encoding.UTF8.GetBytes("e1")].Freq);
            Assert.Equal(0, (int)infos[0][Encoding.UTF8.GetBytes("e1")].Idle);

            var path2 = TestHelper.GetRDBPath("lfu2.rdb");

            var callback2 = new TestReaderCallback(_output);
            var parser2 = new BinaryReaderRDBParser(callback2);
            parser2.Parse(path2);

            var val2 = callback2.GetIdleOrFreq();
            var infos2 = callback2.GetInfos();

            Assert.Equal(2, val2);

            Assert.Equal(1, infos2[0][Encoding.UTF8.GetBytes("e1")].Freq);
            Assert.Equal(0, (int)infos2[0][Encoding.UTF8.GetBytes("e1")].Idle);
        }
    }
}
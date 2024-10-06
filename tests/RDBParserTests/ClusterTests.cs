using RDBParser;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class ClusterTests
    {
        private ITestOutputHelper _output;

        public ClusterTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestSoltInfo()
        {
            // set key{v1} v1
            // set key{v12} v12
            // bgsave
            var path = TestHelper.GetRDBPath("redis74_cluster_slotinfo.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var infos = callback.GetInfos();

            Assert.Equal(1165, (int)infos[0][Encoding.UTF8.GetBytes("key{v1}")].SlotId);
            Assert.Equal(2589, (int)infos[0][Encoding.UTF8.GetBytes("key{v12}")].SlotId);
        }
    }
}
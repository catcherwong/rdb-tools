using RDBParser;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace RDBParserTests
{
    public class SetTests
    {
        private Xunit.Abstractions.ITestOutputHelper _output;

        public SetTests(Xunit.Abstractions.ITestOutputHelper output)
        {
            this._output = output;
        }
       
        [Fact]
        public void TestIntSet16()
        {
            var path = TestHelper.GetRDBPath("intset_16.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("intset_16")]);

            var list = new List<long> { 0x7ffc, 0x7ffd, 0x7ffe };

            var readList = sets[0][Encoding.UTF8.GetBytes("intset_16")];

            foreach (var item in readList)
            {
                Assert.Contains(long.Parse(Encoding.UTF8.GetString(item)), list);
            }
        }

        [Fact]
        public void TestIntSet32()
        {
            var path = TestHelper.GetRDBPath("intset_32.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("intset_32")]);

            var list = new List<long> { 0x7ffefffc, 0x7ffefffd, 0x7ffefffe };

            var readList = sets[0][Encoding.UTF8.GetBytes("intset_32")];

            foreach (var item in readList)
            {
                Assert.Contains(long.Parse(Encoding.UTF8.GetString(item)), list);
            }
        }

        [Fact]
        public void TestIntSet64()
        {
            var path = TestHelper.GetRDBPath("intset_64.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("intset_64")]);

            var list = new List<long> { 0x7ffefffefffefffc, 0x7ffefffefffefffd, 0x7ffefffefffefffe };

            var readList = sets[0][Encoding.UTF8.GetBytes("intset_64")];

            foreach (var item in readList)
            {
                Assert.Contains(long.Parse(Encoding.UTF8.GetString(item)), list);
            }
        }

        [Fact]
        public void TestRegularSet()
        {
            var path = TestHelper.GetRDBPath("regular_set.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(6, lengths[0][Encoding.UTF8.GetBytes("regular_set")]);

            foreach (var item in new List<string> { "alpha", "beta", "gamma", "delta", "phi", "kappa" })
            {
                Assert.Contains(Encoding.UTF8.GetBytes(item), sets[0][Encoding.UTF8.GetBytes("regular_set")]);
            }
        }        
    }
}
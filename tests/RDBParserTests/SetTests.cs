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

            Assert.Contains(RedisRdbObjectHelper.ConvertInt32ToBytes(0x7ffc), sets[0][Encoding.UTF8.GetBytes("intset_16")]);
            Assert.Contains(RedisRdbObjectHelper.ConvertInt32ToBytes(0x7ffd), sets[0][Encoding.UTF8.GetBytes("intset_16")]);
            Assert.Contains(RedisRdbObjectHelper.ConvertInt32ToBytes(0x7ffe), sets[0][Encoding.UTF8.GetBytes("intset_16")]);
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
            
            Assert.Contains(RedisRdbObjectHelper.ConvertInt32ToBytes(0x7ffefffc), sets[0][Encoding.UTF8.GetBytes("intset_32")]);
            Assert.Contains(RedisRdbObjectHelper.ConvertInt32ToBytes(0x7ffefffd), sets[0][Encoding.UTF8.GetBytes("intset_32")]);
            Assert.Contains(RedisRdbObjectHelper.ConvertInt32ToBytes(0x7ffefffe), sets[0][Encoding.UTF8.GetBytes("intset_32")]);
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

            Assert.Contains(System.BitConverter.GetBytes(0x7ffefffefffefffc), sets[0][Encoding.UTF8.GetBytes("intset_64")]);
            Assert.Contains(System.BitConverter.GetBytes(0x7ffefffefffefffd), sets[0][Encoding.UTF8.GetBytes("intset_64")]);
            Assert.Contains(System.BitConverter.GetBytes(0x7ffefffefffefffe), sets[0][Encoding.UTF8.GetBytes("intset_64")]);
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
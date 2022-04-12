using RDBParser;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace RDBParserTests
{
    public class HashTests
    {
        private ITestOutputHelper _output;

        public HashTests(ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestZipMapThatsCompressesEasily()
        {
            var path = TestHelper.GetRDBPath("zipmap_that_compresses_easily.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var hashs = callback.GetHashs();

            Assert.Equal(Encoding.UTF8.GetBytes("aa"), hashs[0][Encoding.UTF8.GetBytes("zipmap_compresses_easily")][Encoding.UTF8.GetBytes("a")]);
            Assert.Equal(Encoding.UTF8.GetBytes("aaaa"), hashs[0][Encoding.UTF8.GetBytes("zipmap_compresses_easily")][Encoding.UTF8.GetBytes("aa")]);
            Assert.Equal(Encoding.UTF8.GetBytes("aaaaaaaaaaaaaa"), hashs[0][Encoding.UTF8.GetBytes("zipmap_compresses_easily")][Encoding.UTF8.GetBytes("aaaaa")]);
        }

        [Fact]
        public void TestZipMapThatDoesntCompress()
        {
            var path = TestHelper.GetRDBPath("zipmap_that_doesnt_compress.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var hashs = callback.GetHashs();

            Assert.Equal(Encoding.UTF8.GetBytes("2"), hashs[0][Encoding.UTF8.GetBytes("zimap_doesnt_compress")][Encoding.UTF8.GetBytes("MKD1G6")]);
            Assert.Equal(Encoding.UTF8.GetBytes("F7TI"), hashs[0][Encoding.UTF8.GetBytes("zimap_doesnt_compress")][Encoding.UTF8.GetBytes("YNNXK")]);
        }

        [Fact]
        public void TestZipMapWithBigValues()
        {
            var path = TestHelper.GetRDBPath("zipmap_with_big_values.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var hashs = callback.GetHashs();

            Assert.Equal(253, hashs[0][Encoding.UTF8.GetBytes("zipmap_with_big_values")][Encoding.UTF8.GetBytes("253bytes")].Length);
            Assert.Equal(254, hashs[0][Encoding.UTF8.GetBytes("zipmap_with_big_values")][Encoding.UTF8.GetBytes("254bytes")].Length);
            Assert.Equal(255, hashs[0][Encoding.UTF8.GetBytes("zipmap_with_big_values")][Encoding.UTF8.GetBytes("255bytes")].Length);
            Assert.Equal(300, hashs[0][Encoding.UTF8.GetBytes("zipmap_with_big_values")][Encoding.UTF8.GetBytes("300bytes")].Length);
            Assert.Equal(20000, hashs[0][Encoding.UTF8.GetBytes("zipmap_with_big_values")][Encoding.UTF8.GetBytes("20kbytes")].Length);
        }

        [Fact]
        public void TestHashAsZipList()
        {
            var path = TestHelper.GetRDBPath("hash_as_ziplist.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var hashs = callback.GetHashs();

            Assert.Equal(Encoding.UTF8.GetBytes("aa"), hashs[0][Encoding.UTF8.GetBytes("zipmap_compresses_easily")][Encoding.UTF8.GetBytes("a")]);
            Assert.Equal(Encoding.UTF8.GetBytes("aaaa"), hashs[0][Encoding.UTF8.GetBytes("zipmap_compresses_easily")][Encoding.UTF8.GetBytes("aa")]);
            Assert.Equal(Encoding.UTF8.GetBytes("aaaaaaaaaaaaaa"), hashs[0][Encoding.UTF8.GetBytes("zipmap_compresses_easily")][Encoding.UTF8.GetBytes("aaaaa")]);
        }

        [Fact]
        public void TestDictionary()
        {
            var path = TestHelper.GetRDBPath("dictionary.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var hashs = callback.GetHashs();
            var lengths = callback.GetLengths();

            Assert.Equal(1000, lengths[0][Encoding.UTF8.GetBytes("force_dictionary")]);
            Assert.Equal(Encoding.UTF8.GetBytes("T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI"), hashs[0][Encoding.UTF8.GetBytes("force_dictionary")][Encoding.UTF8.GetBytes("ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ")]);
            Assert.Equal(Encoding.UTF8.GetBytes("6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ"), hashs[0][Encoding.UTF8.GetBytes("force_dictionary")][Encoding.UTF8.GetBytes("UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491")]);
        }

        [Fact]
        public void TestHashWithRedis70RC3()
        {
            // hset myhash f1 v1
            // hset myhash f2 v2            
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70rc3_with_hash_listpack.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var hashs = callback.GetHashs();

            Assert.Equal(2, lengths[0][Encoding.UTF8.GetBytes("myhash")]);

            Assert.Equal(Encoding.UTF8.GetBytes("v1"), hashs[0][Encoding.UTF8.GetBytes("myhash")][Encoding.UTF8.GetBytes("f1")]);
            Assert.Equal(Encoding.UTF8.GetBytes("v2"), hashs[0][Encoding.UTF8.GetBytes("myhash")][Encoding.UTF8.GetBytes("f2")]);
        }

        [Fact]
        public void TestHashWithRedis70RC3AndIneeger()
        {
            // hset myhash f1 100
            // hset myhash f2 -100
            // hset myhash f3 -9090909
            // hset myhash f4 9090909
            // bgsave
            var path = TestHelper.GetRDBPath("redis_70rc3_with_hash_listpack_and_integer.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var lengths = callback.GetLengths();
            var hashs = callback.GetHashs();

            Assert.Equal(4, lengths[0][Encoding.UTF8.GetBytes("myhash")]);

            Assert.Equal(RedisRdbObjectHelper.LpConvertInt64ToBytes(100), hashs[0][Encoding.UTF8.GetBytes("myhash")][Encoding.UTF8.GetBytes("f1")]);
            Assert.Equal(RedisRdbObjectHelper.LpConvertInt64ToBytes(-100), hashs[0][Encoding.UTF8.GetBytes("myhash")][Encoding.UTF8.GetBytes("f2")]);
            Assert.Equal(RedisRdbObjectHelper.LpConvertInt64ToBytes(-9090909), hashs[0][Encoding.UTF8.GetBytes("myhash")][Encoding.UTF8.GetBytes("f3")]);
            Assert.Equal(RedisRdbObjectHelper.LpConvertInt64ToBytes(9090909), hashs[0][Encoding.UTF8.GetBytes("myhash")][Encoding.UTF8.GetBytes("f4")]);
        }
    }
}
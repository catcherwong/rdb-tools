using RDBParser;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace RDBParserTests
{
    public class BinaryReaderRDBParserTests
    {
        private Xunit.Abstractions.ITestOutputHelper _output;

        public BinaryReaderRDBParserTests(Xunit.Abstractions.ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestEmptyRDB()
        {
            var path = TestHelper.GetRDBPath("empty_database.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            Assert.Contains("StartRDB", callback.GetMethodsCalled());
            Assert.Contains("EndRDB", callback.GetMethodsCalled());

            Assert.Empty(callback.GetDatabases());
        }

        [Fact]
        public void TestMultipleDatabases()
        {
            var path = TestHelper.GetRDBPath("multiple_databases.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();

            Assert.Equal(2, databases.Count);
            Assert.DoesNotContain(1, databases.Keys);

            var v0 = databases[0][Encoding.UTF8.GetBytes("key_in_zeroth_database")];
            var v2 = databases[2][Encoding.UTF8.GetBytes("key_in_second_database")];

            Assert.Equal(Encoding.UTF8.GetBytes("zero"), v0);
            Assert.Equal(Encoding.UTF8.GetBytes("second"), v2);
        }

        [Fact]
        public void TestKeysWithExpiry()
        {
            var path = TestHelper.GetRDBPath("keys_with_expiry.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var expiries = callback.GetExpiries();
            var expiry = expiries[0][Encoding.UTF8.GetBytes("expires_ms_precision")];
            var datetime = System.DateTimeOffset.FromUnixTimeMilliseconds(expiry);

            Assert.Equal(1671963072573, expiry);
            Assert.Equal(2022, datetime.Year);
            Assert.Equal(12, datetime.Month);
            Assert.Equal(25, datetime.Day);
            Assert.Equal(10, datetime.Hour);
            Assert.Equal(11, datetime.Minute);
            Assert.Equal(12, datetime.Second);
            Assert.Equal(573, datetime.Millisecond);
        }

        [Fact]
        public void TestIntegerKeys()
        {
            var path = TestHelper.GetRDBPath("integer_keys.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
            
            Assert.Equal(Encoding.UTF8.GetBytes("Positive 8 bit integer"), databases[0][new byte[]{125}]);
            Assert.Equal(Encoding.UTF8.GetBytes("Positive 16 bit integer"), databases[0][System.BitConverter.GetBytes(0xABAB)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Positive 32 bit integer"), databases[0][System.BitConverter.GetBytes(0x0AEDD325)]);

            Assert.Equal(Encoding.UTF8.GetBytes("Negative 8 bit integer"), databases[0][TestHelper.GetNegativeNumberBytes(-123)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Negative 16 bit integer"), databases[0][TestHelper.GetNegativeNumberBytes(-0x7325)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Negative 32 bit integer"), databases[0][TestHelper.GetNegativeNumberBytes(-0x0AEDD325)]);
        }

        [Fact]
        public void TestRdbVersion8WithModule()
        {
            var path = TestHelper.GetRDBPath("redis_40_with_module.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
            var res = databases[0][Encoding.UTF8.GetBytes("foo")];
            Assert.Equal(Encoding.UTF8.GetBytes("ReJSON-RL"), res);
        }

        [Fact]
        public void TestStringKeyWithCompression()
        {
            var path = TestHelper.GetRDBPath("easily_compressible_string_key.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
            var key = string.Join("", Enumerable.Range(1, 200).Select(x => "a"));
            var res = databases[0][Encoding.UTF8.GetBytes(key)];
            Assert.Equal(Encoding.UTF8.GetBytes("Key that redis should compress easily"), res);
        }

        [Fact]
        public void TestZipMapThatsCompressesEasily()
        {
            var path = TestHelper.GetRDBPath("zipmap_that_compresses_easily.rdb");

            var callback = new TestBinaryReaderCallback(_output);
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

            var callback = new TestBinaryReaderCallback(_output);
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

            var callback = new TestBinaryReaderCallback(_output);
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

            var callback = new TestBinaryReaderCallback(_output);
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

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var hashs = callback.GetHashs();
            var lengths = callback.GetLengths();

            Assert.Equal(1000, lengths[0][Encoding.UTF8.GetBytes("force_dictionary")]);
            Assert.Equal(Encoding.UTF8.GetBytes("T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI"), hashs[0][Encoding.UTF8.GetBytes("force_dictionary")][Encoding.UTF8.GetBytes("ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ")]);
            Assert.Equal(Encoding.UTF8.GetBytes("6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ"), hashs[0][Encoding.UTF8.GetBytes("force_dictionary")][Encoding.UTF8.GetBytes("UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491")]);
        }
    }
}
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

        [Fact]
        public void TestZiplistThatCompressesEasily()
        {
            var path = TestHelper.GetRDBPath("ziplist_that_compresses_easily.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(6, lengths[0][Encoding.UTF8.GetBytes("ziplist_compresses_easily")]);

            int idx = 0;
            foreach (var item in new List<int> { 6, 12, 18 , 24,30,36 })
            {
                var val = string.Join("", Enumerable.Range(1, item).Select(x => "a"));
                var real = sets[0][Encoding.UTF8.GetBytes("ziplist_compresses_easily")];
                Assert.Equal(val, Encoding.UTF8.GetString(real[idx]));
                idx++;
            }
        }

        [Fact]
        public void TestZiplistThatDoesntCompress()
        {
            var path = TestHelper.GetRDBPath("ziplist_that_doesnt_compress.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(2, lengths[0][Encoding.UTF8.GetBytes("ziplist_doesnt_compress")]);
            Assert.Contains(Encoding.UTF8.GetBytes("aj2410"), sets[0][Encoding.UTF8.GetBytes("ziplist_doesnt_compress")]);
            Assert.Contains(Encoding.UTF8.GetBytes("cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344"), sets[0][Encoding.UTF8.GetBytes("ziplist_doesnt_compress")]);            
        }

        [Fact]
        public void TestZiplistWithIntegers()
        {
            var path = TestHelper.GetRDBPath("ziplist_with_integers.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            var list = new List<long> 
            {
                0,1,2,3,4,5,6,7,8,9,10,11,12,-2,13,25,-61,63,16380,-1600,65535,-65523,4194304,9223372036854775807
            };


            Assert.Equal(list.Count, lengths[0][Encoding.UTF8.GetBytes("ziplist_with_integers")]);

            // TODO
            Assert.Contains(new byte[] { 0 }, sets[0][Encoding.UTF8.GetBytes("ziplist_with_integers")]);
            Assert.Contains(new byte[] { 1 }, sets[0][Encoding.UTF8.GetBytes("ziplist_with_integers")]);
        }

        [Fact]
        public void TestLinkedList()
        {
            var path = TestHelper.GetRDBPath("linkedlist.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();
          
            Assert.Equal(1000, lengths[0][Encoding.UTF8.GetBytes("force_linkedlist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ"), sets[0][Encoding.UTF8.GetBytes("force_linkedlist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S"), sets[0][Encoding.UTF8.GetBytes("force_linkedlist")]);
        }

        [Fact]
        public void TestIntSet16()
        {
            var path = TestHelper.GetRDBPath("intset_16.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("intset_16")]);
            
            // TODO
        }

        [Fact]
        public void TestIntSet32()
        {
            var path = TestHelper.GetRDBPath("intset_32.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("intset_32")]);

            // TODO
        }

        [Fact]
        public void TestIntSet64()
        {
            var path = TestHelper.GetRDBPath("intset_64.rdb");

            var callback = new TestBinaryReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(3, lengths[0][Encoding.UTF8.GetBytes("intset_64")]);

            // TODO
        }

        [Fact]
        public void TestRegularSet()
        {
            var path = TestHelper.GetRDBPath("regular_set.rdb");

            var callback = new TestBinaryReaderCallback(_output);
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
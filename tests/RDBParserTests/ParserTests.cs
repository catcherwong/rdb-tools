using RDBParser;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace RDBParserTests
{
    public partial class ParserTests
    {
        private Xunit.Abstractions.ITestOutputHelper _output;

        public ParserTests(Xunit.Abstractions.ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public void TestEmptyRDB()
        {
            var path = TestHelper.GetRDBPath("empty_database.rdb");

            var callback = new TestReaderCallback(_output);
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

            var callback = new TestReaderCallback(_output);
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

            var callback = new TestReaderCallback(_output);
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

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();

            Assert.Equal(Encoding.UTF8.GetBytes("Positive 8 bit integer"), databases[0][RedisRdbObjectHelper.ConvertIntegerToBytes(125)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Positive 16 bit integer"), databases[0][RedisRdbObjectHelper.ConvertIntegerToBytes(0xABAB)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Positive 32 bit integer"), databases[0][RedisRdbObjectHelper.ConvertIntegerToBytes(0x0AEDD325)]);

            Assert.Equal(Encoding.UTF8.GetBytes("Negative 8 bit integer"), databases[0][RedisRdbObjectHelper.ConvertIntegerToBytes(-123)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Negative 16 bit integer"), databases[0][RedisRdbObjectHelper.ConvertIntegerToBytes(-0x7325)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Negative 32 bit integer"), databases[0][RedisRdbObjectHelper.ConvertIntegerToBytes(-0x0AEDD325)]);
        }

        [Fact]
        public void TestStringKeyWithCompression()
        {
            var path = TestHelper.GetRDBPath("easily_compressible_string_key.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
            var key = string.Join("", Enumerable.Range(1, 200).Select(x => "a"));
            var res = databases[0][Encoding.UTF8.GetBytes(key)];
            Assert.Equal(Encoding.UTF8.GetBytes("Key that redis should compress easily"), res);
        }

        [Fact]
        public void TestZiplistThatCompressesEasily()
        {
            var path = TestHelper.GetRDBPath("ziplist_that_compresses_easily.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(6, lengths[0][Encoding.UTF8.GetBytes("ziplist_compresses_easily")]);

            int idx = 0;
            foreach (var item in new List<int> { 6, 12, 18, 24, 30, 36 })
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

            var callback = new TestReaderCallback(_output);
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

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            var list = new List<long>
            {
                0,1,2,3,4,5,6,7,8,9,10,11,12,-2,13,25,-61,63,16380,-16000,65535,-65523,4194304, 9223372036854775807
            };

            Assert.Equal(24, lengths[0][Encoding.UTF8.GetBytes("ziplist_with_integers")]);

            var readList = sets[0][Encoding.UTF8.GetBytes("ziplist_with_integers")];

            foreach (var item in readList)
            {
                Assert.Contains(RedisRdbObjectHelper.ConvertBytesToInteger(item), list);
            }
        }

        [Fact]
        public void TestLinkedList()
        {
            var path = TestHelper.GetRDBPath("linkedlist.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var sets = callback.GetSets();
            var lengths = callback.GetLengths();

            Assert.Equal(1000, lengths[0][Encoding.UTF8.GetBytes("force_linkedlist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ"), sets[0][Encoding.UTF8.GetBytes("force_linkedlist")]);
            Assert.Contains(Encoding.UTF8.GetBytes("TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S"), sets[0][Encoding.UTF8.GetBytes("force_linkedlist")]);
        }
        
        [Fact]
        public void TestRdbVersion5WithChecksum()
        {
            var path = TestHelper.GetRDBPath("rdb_version_5_with_checksum.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
           
            Assert.DoesNotContain(1, databases.Keys);
            Assert.Equal(Encoding.UTF8.GetBytes("efgh"), databases[0][Encoding.UTF8.GetBytes("abcd")]);
            Assert.Equal(Encoding.UTF8.GetBytes("bar"), databases[0][Encoding.UTF8.GetBytes("foo")]);
            Assert.Equal(Encoding.UTF8.GetBytes("baz"), databases[0][Encoding.UTF8.GetBytes("bar")]);
            Assert.Equal(Encoding.UTF8.GetBytes("abcdef"), databases[0][Encoding.UTF8.GetBytes("abcdef")]);
            Assert.Equal(Encoding.UTF8.GetBytes("thisisalongerstring.idontknowwhatitmeans"), databases[0][Encoding.UTF8.GetBytes("longerstring")]);
        }

        [Fact]
        public void TestRdbVersion8With64bLengthAndScores()
        {
            var path = TestHelper.GetRDBPath("rdb_version_8_with_64b_length_and_scores.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
            var sortedSets = callback.GetSortedSets();
            Assert.Equal(Encoding.UTF8.GetBytes("bar"), databases[0][Encoding.UTF8.GetBytes("foo")]);

            var zset = sortedSets[0][Encoding.UTF8.GetBytes("bigset")];
            Assert.Equal(1000, zset.Count);
            Assert.True(TestHelper.FloatEqueal((float)zset[Encoding.UTF8.GetBytes("finalfield")], 2.718f));
        }

        [Fact]
        public void TestMultipleDatabasesStream()
        {
            var path = TestHelper.GetRDBPath("multiple_databases.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();

            Assert.Equal(2, databases.Count);
            Assert.DoesNotContain(1, databases.Keys);

            Assert.Equal(Encoding.UTF8.GetBytes("zero"), databases[0][Encoding.UTF8.GetBytes("key_in_zeroth_database")]);
            Assert.Equal(Encoding.UTF8.GetBytes("second"), databases[2][Encoding.UTF8.GetBytes("key_in_second_database")]);
        }

        [Fact]
        public void TestRdbVersion8WithModule()
        {
            var path = TestHelper.GetRDBPath("redis_40_with_module.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback);
            parser.Parse(path);

            var databases = callback.GetDatabases();
            var res = databases[0][Encoding.UTF8.GetBytes("foo")];
            Assert.Equal(Encoding.UTF8.GetBytes("ReJSON-RL"), res);
        }

        [Fact]
        public void TestFilterWithKeyPrefixes()
        {
            var path = TestHelper.GetRDBPath("multiple_databases.rdb");

            var filter = new ParserFilter()
            {
                KeyPrefixes = new List<string> { "key_in_z" }
            };
            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback, filter);
            parser.Parse(path);

            var databases = callback.GetDatabases();

            Assert.Single(databases[0]);
            Assert.Empty(databases[2]);
        }

        [Fact]
        public void TestFilterWithIsPermanent()
        {
            var path = TestHelper.GetRDBPath("keys_with_expiry.rdb");

            var filter = new ParserFilter()
            {
                IsPermanent = true
            };
            var callback = new TestReaderCallback(_output);
            var parser = new BinaryReaderRDBParser(callback, filter);
            parser.Parse(path);

            var expiries = callback.GetExpiries();
            Assert.Empty(expiries[0]);
        }
    }
}
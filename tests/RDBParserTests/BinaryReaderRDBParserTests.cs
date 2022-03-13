using RDBParser;
using System.Collections.Generic;
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

            Assert.Equal(Encoding.UTF8.GetBytes("Negative 8 bit integer"), databases[0][GetNegativeNumberBytes(-123)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Negative 16 bit integer"), databases[0][GetNegativeNumberBytes(-0x7325)]);
            Assert.Equal(Encoding.UTF8.GetBytes("Negative 32 bit integer"), databases[0][GetNegativeNumberBytes(-0x0AEDD325)]);
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

        private byte[] GetNegativeNumberBytes(int num)
        {
            var tmp = System.BitConverter.GetBytes(num);
            var bytes = new List<byte>();
            foreach(var item in tmp)
            {
                if(item != 255)
                {
                    bytes.Add((byte)(item + 256));
                }
            }
            
            return bytes.ToArray();
        }
    }
}
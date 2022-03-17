using RDBParser;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RDBParserTests
{
    public class PipeReaderRDBParserTests
    {
        private Xunit.Abstractions.ITestOutputHelper _output;

        public PipeReaderRDBParserTests(Xunit.Abstractions.ITestOutputHelper output)
        {
            this._output = output;
        }

        [Fact]
        public async Task TestEmptyRDB()
        {
            var path = TestHelper.GetRDBPath("empty_database.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new PipeReaderRDBParser(callback);
            await parser.ParseAsync(path);

            Assert.Contains("StartRDB", callback.GetMethodsCalled());
            Assert.Contains("EndRDB", callback.GetMethodsCalled());

            Assert.Empty(callback.GetDatabases());
        }

        [Fact]
        public async Task TestMultipleDatabases()
        {
            var path = TestHelper.GetRDBPath("multiple_databases.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new PipeReaderRDBParser(callback);
            await parser.ParseAsync(path);

            var databases = callback.GetDatabases();

            Assert.Equal(2, databases.Count);
            Assert.DoesNotContain(1, databases.Keys);

            var v0 = databases[0][Encoding.UTF8.GetBytes("key_in_zeroth_database")];
            var v2 = databases[2][Encoding.UTF8.GetBytes("key_in_second_database")];

            Assert.Equal(Encoding.UTF8.GetBytes("zero"), v0);
            Assert.Equal(Encoding.UTF8.GetBytes("second"), v2);
        }

        [Fact]
        public async Task TestKeysWithExpiry()
        {
            var path = TestHelper.GetRDBPath("keys_with_expiry.rdb");

            var callback = new TestReaderCallback(_output);
            var parser = new PipeReaderRDBParser(callback);
            await parser.ParseAsync(path);

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

        // [Fact]
        // public void TestRdbVersion8WithModule()
        // {
        //     var path = TestHelper.GetRDBPath("redis_40_with_module.rdb");

        //     var callback = new TestPipeReaderCallback();
        //     var parser = new PipeReaderRDBParser(callback);
        //     await parser.ParseAsync(path);

        //     var databases = callback.GetDatabases();
        //     var res = databases[0][Encoding.UTF8.GetBytes("foo")];
        //     Assert.Equal(Encoding.UTF8.GetBytes("ReJSON-RL"), res);
        // }
    }
}
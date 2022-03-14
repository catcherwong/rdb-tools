using RDBParser;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace RDBParserTests
{
    public class TestPipeReaderCallback : IPipeReaderCallback
    {

        private Xunit.Abstractions.ITestOutputHelper _output;
        private int _database = 0;
        private List<string> _methodsCalled = new List<string>();
        private Dictionary<int, Dictionary<byte[], byte[]>> _databases = new Dictionary<int, Dictionary<byte[], byte[]>>();
        private Dictionary<int, Dictionary<byte[], long>> _expiries = new Dictionary<int, Dictionary<byte[], long>>();

        public TestPipeReaderCallback(Xunit.Abstractions.ITestOutputHelper output)
        {
            this._output = output;
        }

        public List<string> GetMethodsCalled()
            => _methodsCalled;

        public Dictionary<int, Dictionary<byte[], byte[]>> GetDatabases()
            => _databases;

        public Dictionary<int, Dictionary<byte[], long>> GetExpiries()
            => _expiries;


        public void AuxField(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value)
        {
            System.Diagnostics.Trace.WriteLine(EncodingExtensions.GetString(Encoding.UTF8, key));
            System.Diagnostics.Trace.WriteLine(EncodingExtensions.GetString(Encoding.UTF8, value));
        }

        public void DbSize(uint dbSize, uint expiresSize)
        {
            System.Diagnostics.Trace.WriteLine($"{nameof(DbSize)}, {DbSize}, {expiresSize}");
        }

        public void EndDatabase(int dbNumber)
        {
            if (dbNumber != _database)
                throw new System.Exception($"start_database called with {_database}, but end_database called {dbNumber} instead");
        }

        public void EndHash(ReadOnlySequence<byte> key)
        {
            throw new System.NotImplementedException();
        }

        public void EndList(ReadOnlySequence<byte> key, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void EndModule(ReadOnlySequence<byte> key, long bufferSize, ReadOnlySequence<byte> buffer)
        {
            throw new System.NotImplementedException();
        }

        public void EndRDB()
        {
            _methodsCalled.Add(nameof(EndRDB));
        }

        public void EndSet(ReadOnlySequence<byte> key)
        {
            throw new System.NotImplementedException();
        }

        public void EndSortedSet(ReadOnlySequence<byte> key)
        {
            throw new System.NotImplementedException();
        }

        public void EndStream(ReadOnlySequence<byte> key, ulong items, string last_entry_id, List<StreamGroup> cgroups)
        {
            throw new System.NotImplementedException();
        }

        public void HandleModuleData(ReadOnlySequence<byte> key, ulong opCode, ReadOnlySequence<byte> data)
        {
            throw new System.NotImplementedException();
        }

        public void HSet(ReadOnlySequence<byte> key, ReadOnlySequence<byte> field, ReadOnlySequence<byte> value)
        {
            throw new System.NotImplementedException();
        }

        public void RPush(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value)
        {
            throw new System.NotImplementedException();
        }

        public void SAdd(ReadOnlySequence<byte> key, ReadOnlySequence<byte> member)
        {
            throw new System.NotImplementedException();
        }

        public void Set(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value, long expiry, Info info)
        {
            _databases[_database][key.ToArray()] = value.ToArray();

            if (expiry > 0)
                _expiries[_database][key.ToArray()] = expiry;
        }

        public void StartDatabase(int database)
        {
            _output.WriteLine($"====db==={database}=======");
            _database = database;
            _databases[_database] = new Dictionary<byte[], byte[]>(ByteArrayComparer.Default);
            _expiries[_database] = new Dictionary<byte[], long>(ByteArrayComparer.Default);
        }

        public void StartHash(ReadOnlySequence<byte> key, long length, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartList(ReadOnlySequence<byte> key, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public bool StartModule(ReadOnlySequence<byte> key, string module_name, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartRDB(int version)
        {
            _methodsCalled.Add(nameof(StartRDB));
        }

        public void StartSet(ReadOnlySequence<byte> key, long cardinality, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartSortedSet(ReadOnlySequence<byte> key, long length, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartStream(ReadOnlySequence<byte> key, long listpacks_count, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StreamListPack(ReadOnlySequence<byte> key, ReadOnlySequence<byte> entry_id, ReadOnlySequence<byte> data)
        {
            throw new System.NotImplementedException();
        }

        public void ZAdd(ReadOnlySequence<byte> key, double score, ReadOnlySequence<byte> member)
        {
            throw new System.NotImplementedException();
        }
    }
}
using RDBParser;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace RDBParserTests
{
    public class TestBinaryReaderCallback : IBinaryReaderCallback
    {
        private int _database = 0;
        private List<string> _methodsCalled = new List<string>();
        private Dictionary<int, Dictionary<byte[], byte[]>> _databases = new Dictionary<int, Dictionary<byte[], byte[]>>();
        private Dictionary<int, Dictionary<byte[], long>> _expiries = new Dictionary<int, Dictionary<byte[], long>>();

        public List<string> GetMethodsCalled()
            => _methodsCalled;

        public Dictionary<int, Dictionary<byte[], byte[]>> GetDatabases()
            => _databases;

        public Dictionary<int, Dictionary<byte[], long>> GetExpiries()
            => _expiries;

        public void AuxField(byte[] key, byte[] value)
        {
            System.Diagnostics.Trace.WriteLine(System.Text.Encoding.UTF8.GetString(key));
            System.Diagnostics.Trace.WriteLine(System.Text.Encoding.UTF8.GetString(value));
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

        public void EndHash(byte[] key)
        {
            throw new System.NotImplementedException();
        }

        public void EndList(byte[] key, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void EndModule(byte[] key, long bufferSize, byte[] buffer)
        {
            if (!_databases[_database].TryGetValue(key, out _))
            {
                throw new System.Exception("");
            }
        }

        public void EndRDB()
        {
            _methodsCalled.Add(nameof(EndRDB));
        }

        public void EndSet(byte[] key)
        {
            throw new System.NotImplementedException();
        }

        public void EndSortedSet(byte[] key)
        {
            throw new System.NotImplementedException();
        }

        public void EndStream(byte[] key, ulong items, string last_entry_id, List<StreamGroup> cgroups)
        {
            throw new System.NotImplementedException();
        }

        public void HandleModuleData(byte[] key, ulong opCode, byte[] data)
        {
            //throw new System.NotImplementedException();
        }

        public void HSet(byte[] key, byte[] field, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public void RPush(byte[] key, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public void SAdd(byte[] key, byte[] member)
        {
            throw new System.NotImplementedException();
        }

        public void Set(byte[] key, byte[] value, long expiry, Info info)
        {
            _databases[_database][key] = value;

            if (expiry > 0)
                _expiries[_database][key] = expiry;
        }

        public void StartDatabase(int database)
        {
            _database = database;
            _databases[_database] = new Dictionary<byte[], byte[]>(ByteArrayComparer.Default);
            _expiries[_database] = new Dictionary<byte[], long>(ByteArrayComparer.Default);
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            //_databases[_database].ContainsKey(key)
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            if (_databases[_database].TryGetValue(key, out _))
            {
                throw new System.Exception("");
            }
            else
            { 
                _databases[_database][key] = System.Text.Encoding.UTF8.GetBytes(module_name);
            }

            if(expiry > 0) _expiries[_database][key] = expiry;

            return false;
        }

        public void StartRDB(int version)
        {
            _methodsCalled.Add(nameof(StartRDB));
        }

        public void StartSet(byte[] key, long cardinality, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartSortedSet(byte[] key, long length, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
            throw new System.NotImplementedException();
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
            throw new System.NotImplementedException();
        }
    }
}
using RDBParser;
using System.Collections.Generic;
using System.Text;

namespace RDBParserTests
{
    public class TestReaderCallback : IReaderCallback
    {
        private Xunit.Abstractions.ITestOutputHelper _output;
        private int _database = 0;
        private List<string> _methodsCalled = new List<string>();
        private Dictionary<int, Dictionary<byte[], byte[]>> _databases = new Dictionary<int, Dictionary<byte[], byte[]>>();
        private Dictionary<int, Dictionary<byte[], long>> _expiries = new Dictionary<int, Dictionary<byte[], long>>();
        private Dictionary<int, Dictionary<byte[], long>> _lengths = new Dictionary<int, Dictionary<byte[], long>>();
        private Dictionary<int, Dictionary<byte[], Dictionary<byte[], byte[]>>> _hashs = new Dictionary<int, Dictionary<byte[], Dictionary<byte[], byte[]>>>();
        private Dictionary<int, Dictionary<byte[], List<byte[]>>> _sets = new Dictionary<int, Dictionary<byte[], List<byte[]>>>();
        private Dictionary<int, Dictionary<byte[], Dictionary<byte[], double>>> _sortedSets = new Dictionary<int, Dictionary<byte[], Dictionary<byte[], double>>>();
        private Dictionary<int, Dictionary<byte[], List<StreamEntity>>> _streamGroup = new Dictionary<int, Dictionary<byte[], List<StreamEntity>>>();
        private Dictionary<byte[], List<byte[]>> _functions = new Dictionary<byte[], List<byte[]>>(ByteArrayComparer.Default);

        public TestReaderCallback(Xunit.Abstractions.ITestOutputHelper output)
        {
            this._output = output;
        }

        public List<string> GetMethodsCalled()
            => _methodsCalled;

        public Dictionary<int, Dictionary<byte[], byte[]>> GetDatabases()
            => _databases;

        public Dictionary<int, Dictionary<byte[], long>> GetExpiries()
            => _expiries;

        public Dictionary<int, Dictionary<byte[], long>> GetLengths()
            => _lengths;

        public Dictionary<int, Dictionary<byte[], Dictionary<byte[], byte[]>>> GetHashs()
            => _hashs;

        public Dictionary<int, Dictionary<byte[], List<byte[]>>> GetSets()
           => _sets;

        public Dictionary<int, Dictionary<byte[], Dictionary<byte[], double>>> GetSortedSets()
           => _sortedSets;

        public Dictionary<int, Dictionary<byte[], List<StreamEntity>>> GetStreamEntities()
            => _streamGroup;

        public Dictionary<byte[], List<byte[]>> GetFunctions()
          => _functions;

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
            if (!_hashs[_database].ContainsKey(key))
                throw new System.Exception($"start_hash not called for key = {key}");

            if (_hashs[_database][key].Count != _lengths[_database][key])
                throw new System.Exception($"Lengths mismatch on hash {key}, expected length = {_lengths[_database][key]}, actual = {_hashs[_database][key].Count}");
        }

        public void EndList(byte[] key, Info info)
        {
            if (!_sets[_database].ContainsKey(key))
                throw new System.Exception($"start_set not called for key = {key}");

            if (!_lengths.ContainsKey(_database))
            {
                _lengths[_database] = new Dictionary<byte[], long>();
            }

            _lengths[_database][key] = _sets[_database][key].Count;
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
            if (!_sets[_database].ContainsKey(key))
                throw new System.Exception($"start_set not called for key = {key}");

            if (_sets[_database][key].Count != _lengths[_database][key])
                throw new System.Exception($"Lengths mismatch on set {key}, expected length = {_lengths[_database][key]}, actual = {_sets[_database][key].Count}");
        }

        public void EndSortedSet(byte[] key)
        {
            if (!_sortedSets[_database].ContainsKey(key))
                throw new System.Exception($"start_set not called for key = {key}");

            if (_sortedSets[_database][key].Count != _lengths[_database][key])
                throw new System.Exception($"Lengths mismatch on hash {key}, expected length = {_lengths[_database][key]}, actual = {_sortedSets[_database][key].Count}");
        }

        public void EndStream(byte[] key, StreamEntity entity)
        {
            if (!_streamGroup[_database].ContainsKey(key))
                throw new System.Exception($"start_stream not called for key = {key}");

            _lengths[_database][key] = (long)entity.Length;

            _streamGroup[_database][key].Add(entity);
        }

        public void HandleModuleData(byte[] key, ulong opCode, byte[] data)
        {
            //throw new System.NotImplementedException();
        }

        public void HSet(byte[] key, byte[] field, byte[] value)
        {
            if (!_hashs[_database].ContainsKey(key))
                throw new System.Exception("0");

            _output.WriteLine(Encoding.UTF8.GetString(key));
            _output.WriteLine(Encoding.UTF8.GetString(field));
            _output.WriteLine(Encoding.UTF8.GetString(value));
            _hashs[_database][key][field] = value;
        }

        public void RPush(byte[] key, byte[] value)
        {
            if (!_sets[_database].ContainsKey(key))
                throw new System.Exception($"start_list not called for key={key}");

            _sets[_database][key].Add(value);
        }

        public void SAdd(byte[] key, byte[] member)
        {
            if (!_sets[_database].ContainsKey(key))
                throw new System.Exception($"start_set not called for key = {key}");

            _sets[_database][key].Add(member);
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
            _lengths[_database] = new Dictionary<byte[], long>(ByteArrayComparer.Default);
            _hashs[_database] = new Dictionary<byte[], Dictionary<byte[], byte[]>>(ByteArrayComparer.Default);
            _sets[_database] = new Dictionary<byte[], List<byte[]>>(ByteArrayComparer.Default);
            _sortedSets[_database] = new Dictionary<byte[], Dictionary<byte[], double>>(ByteArrayComparer.Default);
            _streamGroup[_database] = new Dictionary<byte[], List<StreamEntity>>(ByteArrayComparer.Default);
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            if (_hashs[_database].ContainsKey(key))
                throw new System.Exception("0");

            _hashs[_database][key] = new Dictionary<byte[], byte[]>(ByteArrayComparer.Default);

            if (expiry > 0)
                _expiries[_database][key] = expiry;

            if (!_lengths.ContainsKey(_database))
                _lengths[_database] = new Dictionary<byte[], long>(ByteArrayComparer.Default);

            _lengths[_database][key] = length;
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            if (_sets[_database].ContainsKey(key))
            {
                throw new System.Exception($"start_list called with key {key} that already exists");
            }
            else
            {
                _sets[_database][key] = new List<byte[]>();
            }

            if (expiry > 0)
                _expiries[_database][key] = expiry;

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

            if (expiry > 0) _expiries[_database][key] = expiry;

            return false;
        }

        public void StartRDB(int version)
        {
            _methodsCalled.Add(nameof(StartRDB));
        }

        public void StartSet(byte[] key, long cardinality, long expiry, Info info)
        {
            if (_sets[_database].ContainsKey(key))
            {
                throw new System.Exception($"start_set called with key {key} that already exists");
            }
            else
            {
                _sets[_database][key] = new List<byte[]>();
            }

            if (expiry > 0)
                _expiries[_database][key] = expiry;

            if (!_lengths.ContainsKey(_database))
                _lengths[_database] = new Dictionary<byte[], long>();

            _lengths[_database][key] = cardinality;
        }

        public void StartSortedSet(byte[] key, long length, long expiry, Info info)
        {
            if (_sortedSets[_database].ContainsKey(key))
                throw new System.Exception($"start_sorted_set called with key {key} that already exists");
            else
                _sortedSets[_database][key] = new Dictionary<byte[], double>(ByteArrayComparer.Default);

            if (expiry > 0)
                _expiries[_database][key] = expiry;

            if (!_lengths.ContainsKey(_database))
                _lengths[_database] = new Dictionary<byte[], long>();

            _lengths[_database][key] = length;
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
            if (_streamGroup[_database].ContainsKey(key))
                throw new System.Exception($"start_stream called with key {key} that already exists");
            else
                _streamGroup[_database][key] = new List<StreamEntity>();

            if (expiry > 0)
                _expiries[_database][key] = expiry;
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
            if (!_streamGroup[_database].ContainsKey(key))
                throw new System.Exception("start_stream not called for key = {key}");

            _output.WriteLine(Encoding.UTF8.GetString(key));
            _output.WriteLine(RedisRdbObjectHelper.GetStreamId(entry_id));
            _output.WriteLine(Encoding.UTF8.GetString(data));
            //_hashs[_database][key][entry_id] = data;
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
            if (!_sortedSets[_database].ContainsKey(key))
                throw new System.Exception("start_sorted_set not called for key =");

            _sortedSets[_database][key][member] = score;
        }

        public void FuntionLoad(byte[] engine, byte[] libName, byte[] code)
        {
            if (!_functions.ContainsKey(engine))
            {
                _functions[engine] = new List<byte[]>();
            }

            _functions[engine].Add(libName);
        }
    }
}
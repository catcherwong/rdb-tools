using RDBParser;
using System.Collections.Generic;
using System.CommandLine;

namespace RDBCli.Callbacks
{
    internal class KeysOnlyCallback : RDBParser.IReaderCallback
    {
        private IConsole _console;

        public KeysOnlyCallback(IConsole console)
        {
            this._console = console;
        }

        public void AuxField(byte[] key, byte[] value)
        {
        }

        public void DbSize(uint dbSize, uint expiresSize)
        {
        }

        public void EndDatabase(int dbNumber)
        {
        }

        public void EndHash(byte[] key)
        {
        }

        public void EndList(byte[] key, Info info)
        {
        }

        public void EndModule(byte[] key, long bufferSize, byte[] buffer)
        {
        }

        public void EndRDB()
        {
        }

        public void EndSet(byte[] key)
        {
        }

        public void EndSortedSet(byte[] key)
        {
        }

        public void EndStream(byte[] key, StreamEntity entity)
        {
        }

        public void HandleModuleData(byte[] key, ulong opCode, byte[] data)
        {
        }

        public void HSet(byte[] key, byte[] field, byte[] value)
        {
        }

        public void RPush(byte[] key, byte[] value)
        {
        }

        public void SAdd(byte[] key, byte[] member)
        {
        }

        public void Set(byte[] key, byte[] value, long expiry, Info info)
        {
            _console.WriteLine(System.Text.Encoding.UTF8.GetString(key));
        }

        public void StartDatabase(int database)
        {
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            _console.WriteLine(System.Text.Encoding.UTF8.GetString(key));
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            _console.WriteLine(System.Text.Encoding.UTF8.GetString(key));
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            if (key != null && key.Length > 0)
                _console.WriteLine(System.Text.Encoding.UTF8.GetString(key));

            return false;
        }

        public void StartRDB(int version)
        {
        }

        public void StartSet(byte[] key, long cardinality, long expiry, Info info)
        {
        }

        public void StartSortedSet(byte[] key, long length, long expiry, Info info)
        {
            _console.WriteLine(System.Text.Encoding.UTF8.GetString(key));
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
            _console.WriteLine(System.Text.Encoding.UTF8.GetString(key));
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
        }
    }
}

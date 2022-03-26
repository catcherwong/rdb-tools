using RDBParser;
using System.Collections.Generic;

namespace RDBCli.Callbacks
{
    internal partial class MemoryCallback : IReaderCallback
    {
        // for x64
        private ulong _pointerSize = 8;
        private ulong _longSize = 8;

        private uint _dbExpires = 0;

        public void AuxField(byte[] key, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public void DbSize(uint dbSize, uint expiresSize)
        {
            throw new System.NotImplementedException();
        }

        public void EndDatabase(int dbNumber)
        {
            throw new System.NotImplementedException();
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
            throw new System.NotImplementedException();
        }

        public void EndRDB()
        {
            throw new System.NotImplementedException();
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
            throw new System.NotImplementedException();
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
            throw new System.NotImplementedException();
        }

        public void StartDatabase(int database)
        {
            throw new System.NotImplementedException();
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartRDB(int version)
        {
            throw new System.NotImplementedException();
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

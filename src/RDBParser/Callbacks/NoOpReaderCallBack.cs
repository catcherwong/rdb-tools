using System.Collections.Generic;

namespace RDBParser
{
    public class NoOpReaderCallBack : IReaderCallback
    {
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

        public void FuntionLoad(byte[] engine, byte[] libName, byte[] code)
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
        }

        public void SetIdleOrFreq(int val)
        {
        }

        public void StartDatabase(int database)
        {
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
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
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
        }
    }
}

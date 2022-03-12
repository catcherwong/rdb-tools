using System;
using System.Text;

namespace RDBParser
{
    public class DefaultConsoleBinaryReaderCallBack : IBinaryReaderCallback
    {
        public void AuxField(byte[] key, byte[] value)
        {
            Console.WriteLine($"AuxField, Key={GetString(key)}, value={GetString(value)}");
        }

        public void DbSize(uint dbSize, uint expiresSize)
        {
            Console.WriteLine($"DbSize dbSize={dbSize}, expiresSize={expiresSize}");
        }

        public void EndDatabase(int database)
        {
            Console.WriteLine($"End database = {database}");
        }

        public void EndHash(byte[] key)
        {
            Console.WriteLine($"End Hash, Key={GetString(key)}");
        }

        public void EndList(byte[] key, Info info)
        {
            Console.WriteLine($"End List, Key={GetString(key)}, Info={info}");
        }

        public void EndModule(byte[] key, long bufferSize, byte[] buffer)
        {
            Console.WriteLine($"EndModule, Key={GetString(key)}, bufferSize={bufferSize}, buffer={GetString(buffer)}");
        }

        public void EndRDB()
        {
            Console.WriteLine("End reading RDB");
        }

        public void EndSet(byte[] key)
        {
            Console.WriteLine($"End Set, Key={GetString(key)}");
        }

        public void EndSortedSet(byte[] key)
        {
            Console.WriteLine($"End SortedSet, Key={GetString(key)}");
        }

        public void EndStream(byte[] key, ulong items, string last_entry_id, StreamGroup cgroups)
        {
            throw new NotImplementedException();
        }

        public void HandleModuleData(byte[] key, ulong opCode, byte[] data)
        {
            Console.WriteLine($"HandleModuleData, Key={GetString(key)}, opCode={opCode}, data={GetString(data)}");
        }

        public void HSet(byte[] key, byte[] field, byte[] value)
        {
            Console.WriteLine($"HSet, Key={GetString(key)}, Field={GetString(field)}, Value={GetString(value)}");
        }

        public void RPush(byte[] key, byte[] value)
        {
            Console.WriteLine($"RPush, Key={GetString(key)}, Value={GetString(value)}");
        }

        public void SAdd(byte[] key, byte[] member)
        {
            Console.WriteLine($"SAdd, Key={GetString(key)}, member={GetString(member)}");
        }

        public void Set(byte[] key, byte[] value, long expiry, Info info)
        {
            Console.WriteLine($"Set, Key={GetString(key)}, value={GetString(value)}, expiry={expiry}, Info={info}");
        }

        public void StartDatabase(int database)
        {
            Console.WriteLine($"Start database = {database}");
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            Console.WriteLine($"Start Hash, Key={GetString(key)}, length={length}, expiry={expiry}, Info={info}");
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            Console.WriteLine($"Start List, Key={GetString(key)}, expiry={expiry}, Info={info}");
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            Console.WriteLine($"Start Module, Key={GetString(key)}, module_name={module_name}, expiry={expiry}, Info={info}");
            return false;
        }

        public void StartRDB(int version)
        {
            Console.WriteLine($"Current RDB file version is {version}");
        }

        public void StartSet(byte[] key, long cardinality, long expiry, Info info)
        {
            Console.WriteLine($"Start Set, Key={GetString(key)}, cardinality={cardinality}, expiry={expiry}, Info={info}");
        }

        public void StartSortedSet(byte[] key, long length, long expiry, Info info)
        {
            Console.WriteLine($"Start SortedSet, Key={GetString(key)}, length={length},  expiry={expiry}, Info={info}");
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
            throw new NotImplementedException();
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
            throw new NotImplementedException();
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
            Console.WriteLine($"SAdd, Key={GetString(key)}, score={score}, member={GetString(member)}");
        }

        private string GetString(byte[] bytes)
            => Encoding.UTF8.GetString(bytes);
    }
}

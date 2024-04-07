using System;
using System.Collections.Generic;
using System.Text;

namespace RDBParser
{
    public class DefaultConsoleReaderCallBack : IReaderCallback
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

        public void EndStream(byte[] key, StreamEntity entity)
        {
            Console.WriteLine($"End Stream, Key={GetString(key)}");
        }

        public void FuntionLoad(byte[] engine, byte[] libName, byte[] code)
        {
            Console.WriteLine($"FuntionLoad, engine={GetString(engine)}, libName={GetString(libName)}");
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

        public void SetIdleOrFreq(int val)
        {
            Console.WriteLine($"SetIdleOrFreq, val={val}");
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
            Console.WriteLine($"Start Stream, Key={GetString(key)}, length={listpacks_count},  expiry={expiry}, Info={info}");
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
            Console.WriteLine($"StreamListPack, Key={GetString(key)}, entryId={GetString(entry_id)}, data={GetString(data)}");
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
            Console.WriteLine($"SAdd, Key={GetString(key)}, score={score}, member={GetString(member)}");
        }

        private string GetString(byte[] bytes)
            => Encoding.UTF8.GetString(bytes);
    }
}

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace RDBParser
{
    public class DefaultConsolePipeReaderCallBack : IPipeReaderCallback
    {
        public void AuxField(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value)
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

        public void EndHash(ReadOnlySequence<byte> key)
        {
            Console.WriteLine($"End Hash, Key={GetString(key)}");
        }

        public void EndList(ReadOnlySequence<byte> key, Info info)
        {
            Console.WriteLine($"End List, Key={GetString(key)}, Info={info}");
        }

        public void EndModule(ReadOnlySequence<byte> key, long bufferSize, ReadOnlySequence<byte> buffer)
        {
            Console.WriteLine($"EndModule, Key={GetString(key)}, bufferSize={bufferSize}, buffer={GetString(buffer)}");
        }

        public void EndRDB()
        {
            Console.WriteLine("End reading RDB");
        }

        public void EndSet(ReadOnlySequence<byte> key)
        {
            Console.WriteLine($"End Set, Key={GetString(key)}");
        }

        public void EndSortedSet(ReadOnlySequence<byte> key)
        {
            Console.WriteLine($"End SortedSet, Key={GetString(key)}");
        }

        public void EndStream(ReadOnlySequence<byte> key, ulong items, string last_entry_id, List<StreamGroup> cgroups)
        {
            Console.WriteLine($"End Stream, Key={GetString(key)}, items={items}, lastEntityId={last_entry_id}");
        }

        public void HandleModuleData(ReadOnlySequence<byte> key, ulong opCode, ReadOnlySequence<byte> data)
        {
            Console.WriteLine($"HandleModuleData, Key={GetString(key)}, opCode={opCode}, data={GetString(data)}");
        }

        public void HSet(ReadOnlySequence<byte> key, ReadOnlySequence<byte> field, ReadOnlySequence<byte> value)
        {
            Console.WriteLine($"HSet, Key={GetString(key)}, Field={GetString(field)}, Value={GetString(value)}");
        }

        public void RPush(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value)
        {
            //Console.WriteLine($"RPush, Key={GetString(key)}, Value={GetString(value)}");
            Console.WriteLine($"RPUSh");
        }

        public void SAdd(ReadOnlySequence<byte> key, ReadOnlySequence<byte> member)
        {
            Console.WriteLine($"SAdd, Key={GetString(key)}, member={GetString(member)}");
        }

        public void Set(ReadOnlySequence<byte> key, ReadOnlySequence<byte> value, long expiry, Info info)
        {
            Console.WriteLine($"Set, Key={GetString(key)}, value={GetString(value)}, expiry={expiry}, Info={info}");
        }

        public void StartDatabase(int database)
        {
            Console.WriteLine($"Start database = {database}");
        }

        public void StartHash(ReadOnlySequence<byte> key, long length, long expiry, Info info)
        {
            Console.WriteLine($"Start Hash, Key={GetString(key)}, length={length}, expiry={expiry}, Info={info}");
        }

        public void StartList(ReadOnlySequence<byte> key, long expiry, Info info)
        {
            Console.WriteLine($"Start List, Key={GetString(key)}, expiry={expiry}, Info={info}");
        }

        public bool StartModule(ReadOnlySequence<byte> key, string module_name, long expiry, Info info)
        {
            Console.WriteLine($"Start Module, Key={GetString(key)}, module_name={module_name}, expiry={expiry}, Info={info}");
            return false;
        }

        public void StartRDB(int version)
        {
            Console.WriteLine($"Current RDB file version is {version}");
        }

        public void StartSet(ReadOnlySequence<byte> key, long cardinality, long expiry, Info info)
        {
            Console.WriteLine($"Start Set, Key={GetString(key)}, cardinality={cardinality}, expiry={expiry}, Info={info}");
        }

        public void StartSortedSet(ReadOnlySequence<byte> key, long length, long expiry, Info info)
        {
            Console.WriteLine($"Start SortedSet, Key={GetString(key)}, length={length},  expiry={expiry}, Info={info}");
        }

        public void StartStream(ReadOnlySequence<byte> key, long listpacks_count, long expiry, Info info)
        {
            Console.WriteLine($"Start Stream, Key={GetString(key)}, length={listpacks_count},  expiry={expiry}, Info={info}");
        }

        public void StreamListPack(ReadOnlySequence<byte> key, ReadOnlySequence<byte> entry_id, ReadOnlySequence<byte> data)
        {
            Console.WriteLine($"StreamListPack, Key={GetString(key)}, entryId={GetString(entry_id)}, data={GetString(data)}");
        }

        public void ZAdd(ReadOnlySequence<byte> key, double score, ReadOnlySequence<byte> member)
        {
            Console.WriteLine($"SAdd, Key={GetString(key)}, score={score}, member={GetString(member)}");
        }

        private string GetString(ReadOnlySequence<byte> bytes)
        {
            try
            {
                return EncodingExtensions.GetString(Encoding.UTF8, in bytes);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return string.Empty;
            }
        }
    }
}

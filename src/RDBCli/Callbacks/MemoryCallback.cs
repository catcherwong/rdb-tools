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

        private int _dbNum = 0;

        private RdbDataInfo _rdbDataInfo = new RdbDataInfo();

        private Record _currentRecord = new Record();

        public void AuxField(byte[] key, byte[] value)
        {
            var keyStr = System.Text.Encoding.UTF8.GetString(key);
            if (keyStr.Equals("used-mem"))
            {
                var mem = System.Text.Encoding.UTF8.GetString(value);
                if (long.TryParse(mem, out var usedMem))
                {
                    _rdbDataInfo.UsedMem = usedMem;
                }
            }
            else if (keyStr.Equals("redis-ver"))
            {
                _rdbDataInfo.RedisVer = System.Text.Encoding.UTF8.GetString(value);
            }
            else if (keyStr.Equals("redis-bits"))
            {
                var bits = System.Text.Encoding.UTF8.GetString(value);
                if (long.TryParse(bits, out var redisBits))
                {
                    _rdbDataInfo.RedisBits = redisBits;
                }
            }
            else if (keyStr.Equals("ctime"))
            {
                var time = System.Text.Encoding.UTF8.GetString(value);
                if (long.TryParse(time, out var ctime))
                {
                    _rdbDataInfo.CTime = ctime;
                }
            }
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
            _rdbDataInfo.Records.Add(_currentRecord);
            _rdbDataInfo.Count++;
            _currentRecord = new Record();
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
            var lenOfElem = ElementLength(field) + ElementLength(value);
            if(lenOfElem > _currentRecord.LenOfLargestElem)
            {
                _currentRecord.FieldOfLargestElem = System.Text.Encoding.UTF8.GetString(field);
                _currentRecord.LenOfLargestElem = lenOfElem;
            }

            if(_currentRecord.Encoding.Equals("hashtable"))
            {
                _currentRecord.Bytes += SizeOfString(field);
                _currentRecord.Bytes += SizeOfString(value);
                _currentRecord.Bytes += HashtableEntryOverhead();

                if(_rdbDataInfo.RdbVer < 8)
                {
                    _currentRecord.Bytes += 2 * RobjOverhead();
                }
            }
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
            var bytes = TopLevelObjOverhead(key, expiry) + SizeOfString(value);
            var length = ElementLength(value);

            var record = new Record()
            {
                Type = "string",
                Key = System.Text.Encoding.UTF8.GetString(key),
                Bytes = bytes,
                Encoding = info.Encoding,
                NumOfElem = length,
                Expiry = expiry,
                Database = _dbNum,
            };

            _rdbDataInfo.Records.Add(record);
            _rdbDataInfo.Count++;
        }

        public void StartDatabase(int database)
        {
            _dbNum = database;
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            var keyStr = System.Text.Encoding.UTF8.GetString(key);
            var bytes = TopLevelObjOverhead(key, expiry);

            if(info.SizeOfValue > 0)
            {
                bytes += (ulong)info.SizeOfValue;
            }
            else if(info.Encoding == "hashtable")
            {
                bytes += HashtableOverhead((ulong)length);
            }
            else
            {
                throw new System.Exception("");
            }

            _currentRecord = new Record
            {
                Key = keyStr,
                Bytes = bytes,
                Type = "hash",
                NumOfElem = (ulong)length,
                Encoding = info.Encoding,
                Expiry = expiry,
                Database = _dbNum,
            };
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            throw new System.NotImplementedException();
        }

        public void StartRDB(int version)
        {
            _rdbDataInfo.RdbVer = version;
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

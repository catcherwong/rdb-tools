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
        // For Stream
        private ulong _listpacksCount;

        private RdbDataInfo _rdbDataInfo = new RdbDataInfo();

        private Record _currentRecord = new Record();

        public RdbDataInfo GetRdbDataInfo() => _rdbDataInfo;

        public void AuxField(byte[] key, byte[] value)
        {
            var keyStr = System.Text.Encoding.UTF8.GetString(key);
            if (keyStr.Equals("used-mem"))
            {
                var mem = System.Text.Encoding.UTF8.GetString(value);

                _rdbDataInfo.UsedMem = long.TryParse(mem, out var usedMem)
                    ? usedMem
                    : RedisRdbObjectHelper.ConvertBytesToInteger(value);
            }
            else if (keyStr.Equals("redis-ver"))
            {
                _rdbDataInfo.RedisVer = System.Text.Encoding.UTF8.GetString(value);
            }
            else if (keyStr.Equals("redis-bits"))
            {
                _rdbDataInfo.RedisBits = RedisRdbObjectHelper.ConvertBytesToInteger(value);
            }
            else if (keyStr.Equals("ctime"))
            {
                _rdbDataInfo.CTime = RedisRdbObjectHelper.ConvertBytesToInteger(value);
            }
        }

        public void DbSize(uint dbSize, uint expiresSize)
        {
        }

        public void EndDatabase(int dbNumber)
        {
        }

        public void EndHash(byte[] key)
        {
            _rdbDataInfo.TotalMem += _currentRecord.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(_currentRecord));
            _rdbDataInfo.Count++;
            _currentRecord = null;
        }

        public void EndList(byte[] key, Info info)
        {
            if (_currentRecord.Encoding.Equals("ziplist"))
            {
                _currentRecord.Bytes += ZiplistHeaderOverHead();
            }
            else if (_currentRecord.Encoding.Equals("quicklist"))
            {
                _currentRecord.Bytes += QuicklistOverhead(0);
                _currentRecord.Bytes += ZiplistHeaderOverHead();
            }
            else if (_currentRecord.Encoding.Equals("linkedlist"))
            {
                _currentRecord.Bytes += LinkedlistOverhead();
            }
            else
            {
                throw new System.Exception($"unknown encoding: {_currentRecord.Encoding}");
            }

            _rdbDataInfo.TotalMem += _currentRecord.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(_currentRecord));
            _rdbDataInfo.Count++;
            _currentRecord = null;
        }

        public void EndModule(byte[] key, long bufferSize, byte[] buffer)
        {
            _currentRecord.Bytes += (ulong)bufferSize;

            _rdbDataInfo.TotalMem += _currentRecord.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(_currentRecord));
            _rdbDataInfo.Count++;
            _currentRecord = null;
        }

        public void EndRDB()
        {
            _rdbDataInfo.Records.CompleteAdding();
        }

        public void EndSet(byte[] key)
        {
            _rdbDataInfo.TotalMem += _currentRecord.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(_currentRecord));
            _rdbDataInfo.Count++;
            _currentRecord = null;
        }

        public void EndSortedSet(byte[] key)
        {
            _rdbDataInfo.TotalMem += _currentRecord.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(_currentRecord));
            _rdbDataInfo.Count++;
            _currentRecord = null;
        }

        public void EndStream(byte[] key, StreamEntity entity)
        {
            _currentRecord.NumOfElem = entity.Length;
            _currentRecord.Bytes += SizeofStreamRadixTree(_listpacksCount);

            foreach (var cg in entity.CGroups)
            {
                var pendingLength = (ulong)cg.Pending.Count;
                _currentRecord.Bytes += SizeofStreamRadixTree(pendingLength);
                _currentRecord.Bytes += StreamNACK(pendingLength);
                _currentRecord.Bytes += StreamCG();

                foreach (var c in cg.Consumers)
                {
                    _currentRecord.Bytes += StreamConsumer(c.Name);
                    pendingLength = (ulong)cg.Pending.Count;
                    _currentRecord.Bytes += SizeofStreamRadixTree(pendingLength);
                }
            }

            _rdbDataInfo.TotalMem += _currentRecord.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(_currentRecord, StreamsRecord.MapFromStreamsEntity(entity, key)));
            _rdbDataInfo.Count++;
            _currentRecord = null;
        }

        public void HandleModuleData(byte[] key, ulong opCode, byte[] data)
        {
        }

        public void HSet(byte[] key, byte[] field, byte[] value)
        {
            var lenOfElem = ElementLength(field) + ElementLength(value);
            if (lenOfElem > _currentRecord.LenOfLargestElem)
            {
                _currentRecord.FieldOfLargestElem = System.Text.Encoding.UTF8.GetString(field);
                _currentRecord.LenOfLargestElem = lenOfElem;
            }

            if (_currentRecord.Encoding.Equals("hashtable"))
            {
                _currentRecord.Bytes += SizeOfString(field);
                _currentRecord.Bytes += SizeOfString(value);
                _currentRecord.Bytes += HashtableEntryOverhead();

                if (_rdbDataInfo.RdbVer < 8)
                {
                    _currentRecord.Bytes += 2 * RobjOverhead();
                }
            }
        }

        public void RPush(byte[] key, byte[] value)
        {
            _currentRecord.NumOfElem++;

            if (_currentRecord.Encoding.Equals("ziplist"))
            {
                _currentRecord.Bytes += ZiplistEntryOverhead(value);
            }
            else if (_currentRecord.Encoding.Equals("quicklist"))
            {
                _currentRecord.Bytes += ZiplistEntryOverhead(value);
            }
            else if (_currentRecord.Encoding.Equals("linkedlist"))
            {
                ulong size = 0;
                if (!RDBParser.RedisRdbObjectHelper.IsInt(value, out _))
                {
                    size = SizeOfString(value);
                }

                _currentRecord.Bytes += LinkedlistEntryOverhead();
                _currentRecord.Bytes += size;

                if (_rdbDataInfo.RdbVer < 8)
                {
                    _currentRecord.Bytes += RobjOverhead();
                }
            }
            else
            {
                throw new System.Exception($"unknown encoding: {_currentRecord.Encoding}");
            }

            var lenOfElem = ElementLength(value);
            if (lenOfElem > _currentRecord.LenOfLargestElem)
            {
                _currentRecord.FieldOfLargestElem = System.Text.Encoding.UTF8.GetString(value);
                _currentRecord.LenOfLargestElem = lenOfElem;
            }
        }

        public void SAdd(byte[] key, byte[] member)
        {
            var lenOfElem = ElementLength(member);
            if (lenOfElem > _currentRecord.LenOfLargestElem)
            {
                _currentRecord.FieldOfLargestElem = System.Text.Encoding.UTF8.GetString(member);
                _currentRecord.LenOfLargestElem = lenOfElem;
            }

            if (_currentRecord.Encoding.Equals("hashtable"))
            {
                _currentRecord.Bytes += SizeOfString(member);
                _currentRecord.Bytes += HashtableEntryOverhead();

                if (_rdbDataInfo.RdbVer < 8)
                {
                    _currentRecord.Bytes += RobjOverhead();
                }
            }
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
                LenOfLargestElem = length,
            };

            _rdbDataInfo.TotalMem += record.Bytes;
            _rdbDataInfo.Records.Add(new AnalysisRecord(record));
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

            if (info.SizeOfValue > 0)
            {
                bytes += (ulong)info.SizeOfValue;
            }
            else if (info.Encoding == "hashtable")
            {
                bytes += HashtableOverhead((ulong)length);
            }
            else
            {
                throw new System.Exception($"unexpected size(0) or encoding:{info.Encoding}");
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
            var keyStr = System.Text.Encoding.UTF8.GetString(key);
            var bytes = TopLevelObjOverhead(key, expiry);

            var encoding = info.Encoding;

            _currentRecord = new Record
            {
                Key = keyStr,
                Bytes = bytes,
                Type = "list",
                NumOfElem = 0,
                Encoding = encoding,
                Expiry = expiry,
                Database = _dbNum,
            };
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            var keyStr = key == null ? string.Empty : System.Text.Encoding.UTF8.GetString(key);
            var bytes = key == null ? 0 : TopLevelObjOverhead(key, expiry);

            bytes += 8 + 1;

            _currentRecord = new Record
            {
                Key = keyStr,
                Bytes = bytes,
                Type = "module",
                Encoding = module_name,
                Expiry = expiry,
                NumOfElem = 1,
                Database = _dbNum,
            };

            return false;
        }

        public void StartRDB(int version)
        {
            _rdbDataInfo.RdbVer = version;
        }

        public void StartSet(byte[] key, long cardinality, long expiry, Info info)
        {
            this.StartHash(key, cardinality, expiry, info);
            _currentRecord.Type = "set";
        }

        public void StartSortedSet(byte[] key, long length, long expiry, Info info)
        {
            var keyStr = System.Text.Encoding.UTF8.GetString(key);
            var bytes = TopLevelObjOverhead(key, expiry);

            if (info.SizeOfValue > 0)
            {
                bytes += (ulong)info.SizeOfValue;
            }
            else if (info.Encoding.Equals("skiplist"))
            {
                bytes += SkiplistOverhead((ulong)length);
            }
            else
            {
                throw new System.Exception($"unexpected size(0) or encoding:{info.Encoding}");
            }

            _currentRecord = new Record
            {
                Key = keyStr,
                Bytes = bytes,
                Type = "sortedset",
                NumOfElem = (ulong)length,
                Encoding = info.Encoding,
                Expiry = expiry,
                Database = _dbNum,
            };
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
            var keyStr = System.Text.Encoding.UTF8.GetString(key);
            var bytes = TopLevelObjOverhead(key, expiry);
            bytes += StreamOverhead();
            bytes += RaxOverhead();

            _listpacksCount = (ulong)listpacks_count;
            _currentRecord = new Record
            {
                Key = keyStr,
                Bytes = bytes,
                Type = "stream",
                NumOfElem = 0,
                Encoding = info.Encoding,
                Expiry = expiry,
                Database = _dbNum,
            };
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
            if ((ulong)data.Length > _currentRecord.LenOfLargestElem)
            {
                // _currentRecord.FieldOfLargestElem = System.Text.Encoding.UTF8.GetString(data);
                _currentRecord.LenOfLargestElem = (ulong)data.Length;
            }

            _currentRecord.Bytes += MemProfiler.MallocOverhead((ulong)data.Length);
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
            var lenOfElem = ElementLength(member);
            if (lenOfElem > _currentRecord.LenOfLargestElem)
            {
                _currentRecord.FieldOfLargestElem = System.Text.Encoding.UTF8.GetString(member);
                _currentRecord.LenOfLargestElem = lenOfElem;
            }

            if (_currentRecord.Encoding.Equals("skiplist"))
            {
                _currentRecord.Bytes += 8;
                _currentRecord.Bytes += SizeOfString(member);
                _currentRecord.Bytes += SkiplistEntiryOverhead();

                if (_rdbDataInfo.RdbVer < 8)
                {
                    _currentRecord.Bytes += RobjOverhead();
                }
            }
        }

        public void FuntionLoad(byte[] engine, byte[] libName, byte[] code)
        {
            _rdbDataInfo.Functions.Add(new FunctionsRecord
            {
                Engine = System.Text.Encoding.UTF8.GetString(engine),
                LibraryName = System.Text.Encoding.UTF8.GetString(libName),
            });

            _rdbDataInfo.TotalMem += FunctionOverhead(engine, libName, code);
        }
    }
}

using RDBCli.Callbacks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RDBCli
{
    internal class RdbDataCounter
    {
        private char[] _separators = new char[] { ':', ';', ',', '_', '-', '.' };

        private PriorityQueue<Record, ulong> _largestRecords;
        private PriorityQueue<PrefixRecord, PrefixRecord> _largestKeyPrefixes;
        private PriorityQueue<StreamsRecord, ulong> _largestStreams;
        private Dictionary<string, TypeKeyValue> _keyPrefix;
        private Dictionary<string, CommonStatValue> _typeDict;
        private Dictionary<string, CommonStatValue> _expiryDict;
        private Dictionary<string, CommonStatValue> _idleOrFreqDict;
        private Dictionary<string, CommonStatValue> _dbDict;

        private readonly MemoryCallback _cb;
        private readonly BlockingCollection<AnalysisRecord> _records;
        private readonly int _sepCount;
        private readonly bool _keySuffixEnable;

        public RdbDataCounter(MemoryCallback cb, string separators = "", int sepCount = -1, bool keySuffixEnable = false)
        {
            this._cb = cb;
            this._records = cb.GetRdbDataInfo().Records;
            this._largestRecords = new PriorityQueue<Record, ulong>();
            this._largestStreams = new PriorityQueue<StreamsRecord, ulong>();
            this._largestKeyPrefixes = new PriorityQueue<PrefixRecord, PrefixRecord>(PrefixRecord.Comparer);
            this._keyPrefix = new Dictionary<string, TypeKeyValue>();
            this._typeDict = new Dictionary<string, CommonStatValue>();
            this._expiryDict = new Dictionary<string, CommonStatValue>();
            this._idleOrFreqDict = new Dictionary<string, CommonStatValue>();
            this._dbDict = new Dictionary<string, CommonStatValue>();

            if (!string.IsNullOrWhiteSpace(separators))
            {
                _separators = separators.ToCharArray();
            }

            _sepCount = sepCount > 0 ? sepCount : 1;
            _keySuffixEnable =  keySuffixEnable;
        }

        public Task Count()
        {
            System.Threading.CancellationTokenSource cts = new();
            var task = Task.Factory.StartNew(() => 
            {
                while (!_records.IsCompleted)
                {
                    try
                    {
                        if (_records.TryTake(out var item))
                        {
                            this.CountLargestEntries(item.Record, 500);
                            this.CounteByType(item.Record);
                            this.CounteByIdleOrFreq(item.Record);
                            this.CountByKeyPrefix(item.Record);
                            this.CountExpiry(item.Record);
                            this.CountDb(item.Record);
                            this.CountStreams(item.StreamsRecord, 500);
                        }
                        else
                        {
                            continue;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                CalcuLargestKeyPrefix(500);
                cts.Cancel();
            }, cts.Token);

            return task;
        }

        public List<PrefixRecord> GetLargestKeyPrefixes(int num = 100)
        {
            return _largestKeyPrefixes.UnorderedItems
                .OrderByDescending(x => x.Priority, PrefixRecord.Comparer)
                .Select(x=>x.Element)
                .Take(num)
                .ToList();
        }

        public List<Record> GetLargestRecords(int num = 100)
        {
            return _largestRecords.UnorderedItems
                .OrderByDescending(x => x.Priority)
                .Select(x => x.Element)
                .Take(num)
                .ToList();
        }

        public List<DBRecord> GetDatabaseInfo(int num = 10)
        {
            return _dbDict
               .Select(x => new DBRecord { DB = x.Key, Num = x.Value.Num, Bytes = x.Value.Bytes })
               .OrderByDescending(x => x.Bytes)
               .Take(num)
               .ToList();
        }

        public List<TypeRecord> GetTypeRecords()
        {
            return _typeDict
                .Select(x => new TypeRecord { Type = x.Key, Num = x.Value.Num, Bytes = x.Value.Bytes })
                .ToList();
        }

        public List<ExpiryRecord> GetExpiryInfo()
        {
            return _expiryDict
               .Select(x => new ExpiryRecord { Expiry = x.Key, Num = x.Value.Num, Bytes = x.Value.Bytes })
               .OrderBy(x => x.Expiry)
               .ToList();
        }

        public List<IdleOrFreqRecord> GetIdleOrFreqInfo()
        {
            return _idleOrFreqDict
               .Select(x => new IdleOrFreqRecord { Category = x.Key, Num = x.Value.Num, Bytes = x.Value.Bytes })
               .OrderBy(x => x.Category)
               .ToList();
        }

        public List<StreamsRecord> GetStreamRecords(int num = 100)
        {
            return _largestStreams.UnorderedItems
                .OrderByDescending(x=>x.Priority)
                .Select(x => x.Element)
                .Take(num)
                .ToList();
        }

        private void CountDb(Record record)
        {
            var key = $"db{record.Database}";
            InitOrAddStat(this._dbDict, key, record.Bytes);
        }

        private void CountExpiry(Record item)
        {
            var key = CommonHelper.GetExpireString(item.Expiry);

            InitOrAddStat(this._expiryDict, key, item.Bytes);
        }

        private void CalcuLargestKeyPrefix(int num)
        {
            foreach (var item in _keyPrefix)
            {
                var tk = TypeKey.FromString(item.Key);
                var ent = new PrefixRecord
                {
                    Type = tk.Type,
                    Prefix = tk.Key,
                    Bytes = item.Value.Bytes,
                    Num = item.Value.Num,
                    Elements = item.Value.Elements
                };

                _largestKeyPrefixes.Enqueue(ent, ent);
                if (_largestKeyPrefixes.Count > num)
                {
                    _ = _largestKeyPrefixes.Dequeue();
                }
            }
        }

        private void CountStreams(StreamsRecord streamsRecord, int num)
        {
            if(streamsRecord == null) return;

            _largestStreams.Enqueue(streamsRecord, streamsRecord.Length);

            if (_largestStreams.Count > num)
            {
                _ = _largestStreams.Dequeue();
            }
        }

        private void CountByKeyPrefix(Record record)
        {
            var prefixes = CommonHelper.GetPrefixes(record.Key, _separators, _sepCount, _keySuffixEnable);

            var tKey = new TypeKey { Type = record.Type };

            foreach (var item in prefixes)
            {
                if (item.Length == 0) continue;

                tKey.Key = item;

                if (this._keyPrefix.ContainsKey(tKey.ToString()))
                {
                    this._keyPrefix[tKey.ToString()].Num++;
                    this._keyPrefix[tKey.ToString()].Bytes += record.Bytes;
                    this._keyPrefix[tKey.ToString()].Elements += record.NumOfElem;
                    this._keyPrefix[tKey.ToString()].Idle += record.Idle;
                }
                else
                {
                    this._keyPrefix[tKey.ToString()] = new TypeKeyValue
                    {
                        Num = 1,
                        Bytes = record.Bytes,
                        Elements = record.NumOfElem,
                        Idle = record.Idle
                    };
                }
            }
        }

        private void CountLargestEntries(Record record, int num)
        {
            record.Key = CommonHelper.GetShortKey(record.Key);

            _largestRecords.Enqueue(record, record.Bytes);

            if (_largestRecords.Count > num)
            {
                _ = _largestRecords.Dequeue();
            }
        }

        private void CounteByType(Record record)
        {
            InitOrAddStat(this._typeDict, record.Type, record.Bytes);
        }

        private void CounteByIdleOrFreq(Record record)
        {
            if (_cb.GetIdleOrFreq() == 1)
            {
                var key = CommonHelper.GetIdleString(record.Idle);

                InitOrAddStat(this._idleOrFreqDict, key, record.Bytes);
            }
            else if (_cb.GetIdleOrFreq() == 2)
            {
                var key = CommonHelper.GetFreqString(record.Freq);

                InitOrAddStat(this._idleOrFreqDict, key, record.Bytes);
            }
        }

        private void InitOrAddStat(Dictionary<string, CommonStatValue> dict, string key, ulong bytes)
        {
            if (dict.ContainsKey(key))
            {
                dict[key].Num++;
                dict[key].Bytes += bytes;
            }
            else
            {
                dict[key] = new CommonStatValue
                {
                    Bytes = bytes,
                    Num = 1,
                };
            }
        }
    }
}

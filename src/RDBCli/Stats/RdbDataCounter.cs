using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RDBCli
{
    internal class RdbDataCounter
    {
        private static readonly char[] Separators = new char[] { ':', ';', ',', '_', '-' };

        private PriorityQueue<Record, ulong> _largestRecords;
        private PriorityQueue<PrefixRecord, PrefixRecord> _largestKeyPrefixes;
        private Dictionary<TypeKey, TypeKeyValue> _keyPrefix;
        private Dictionary<string, CommonStatValue> _typeDict;
        private Dictionary<string, CommonStatValue> _expiryDict;

        private readonly BlockingCollection<Record> _records;

        public RdbDataCounter(BlockingCollection<Record> records)
        {
            this._records = records;
            this._largestRecords = new PriorityQueue<Record, ulong>();
            this._largestKeyPrefixes = new PriorityQueue<PrefixRecord, PrefixRecord>(PrefixRecord.Comparer);
            this._keyPrefix = new Dictionary<TypeKey, TypeKeyValue>(TypeKey.Comparer);
            this._typeDict = new Dictionary<string, CommonStatValue>();
            this._expiryDict = new Dictionary<string, CommonStatValue>();
        }

        public Task Count()
        {
            System.Threading.CancellationTokenSource cts = new System.Threading.CancellationTokenSource();
            var task = Task.Factory.StartNew(() => 
            {
                while (!_records.IsAddingCompleted)
                {
                    try
                    {
                        if (_records.TryTake(out var item, 10))
                        {
                            this.CountLargestEntries(item, 10);
                            this.CounteByType(item);
                            this.CountByKeyPrefix(item);
                            this.CountExpiry(item);
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

        private void CountExpiry(Record item)
        {
            var key = string.Empty;

            if (item.Expiry > 0)
            {
                var sub = DateTimeOffset.FromUnixTimeMilliseconds(item.Expiry).Subtract(DateTimeOffset.UtcNow);

                // 0~1h, 1~3h, 3~12h, 12~24h, 24~72h, 72~168h, 168h~
                var hour = sub.TotalHours;
                if(hour <= 0)
                {
                    key = "Already Expired";
                }
                else if (hour > 0 && hour < 1)
                {
                    key = "0~1h";
                }
                else if (hour >= 1 && hour < 3)
                {
                    key = "1~3h";
                }
                else if (hour >= 3 && hour < 12)
                {
                    key = "3~12h";
                }
                else if (hour >= 12 && hour < 24)
                {
                    key = "12~24h";
                }
                else if (hour >= 24 && hour < 72)
                {
                    key = "1~3d";
                }
                else if (hour >= 72 && hour < 168)
                {
                    key = "3~7d";
                }
                else if (hour >= 168)
                {
                    key = ">7d";
                }
            }
            else if (item.Expiry == 0)
            {
                key = "Permanent";
            }
            else
            {
                key = item.Expiry.ToString();
            }

            InitOrAddStat(this._expiryDict, key, item.Bytes);
        }

        private void CalcuLargestKeyPrefix(int num)
        {
            foreach (var item in _keyPrefix)
            {
                var ent = new PrefixRecord
                {
                    Type = item.Key.Type,
                    Prefix = item.Key.Key,
                    Bytes = item.Value.Bytes,
                    Num = item.Value.Num,
                    Elements = item.Value.Elements,
                };

                _largestKeyPrefixes.Enqueue(ent, ent);
                if (_largestKeyPrefixes.Count > num)
                {
                    _ = _largestKeyPrefixes.Dequeue();
                }
            }        
        }

        private void CountByKeyPrefix(Record record)
        {
            var prefixes = GetPrefixes(record.Key);

            var tKey = new TypeKey { Type = record.Type };

            foreach (var item in prefixes)
            {
                if (item.Length == 0) continue;

                tKey.Key = item;

                if (this._keyPrefix.ContainsKey(tKey))
                {
                    this._keyPrefix[tKey].Num++;
                    this._keyPrefix[tKey].Bytes += record.Bytes;
                    this._keyPrefix[tKey].Elements += record.NumOfElem;
                }
                else
                {
                    this._keyPrefix[tKey] = new TypeKeyValue
                    {
                        Num = 1,
                        Bytes = record.Bytes,
                        Elements = record.NumOfElem
                    };
                }
            }
        }

        private List<string> GetPrefixes(string s)
        {
            var res = new List<string>();

            var span = s.AsSpan();

            var sepIdx = span.IndexOfAny(Separators);

            if (sepIdx < 0) res.Add(s);

            if (sepIdx > -1)
            {
                var str = new string(span[..(sepIdx + 1)]);
                res.Add(str.TrimEnd(Separators));
            }

            return res;
        }

        private void CountLargestEntries(Record record, int num)
        {
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

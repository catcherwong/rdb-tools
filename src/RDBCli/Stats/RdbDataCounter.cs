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
        private Dictionary<string, TypeStatValue> _typeDict;
        private Dictionary<string, ulong> _expiryDict;

        private readonly BlockingCollection<Record> _records;

        public RdbDataCounter(BlockingCollection<Record> records)
        {
            this._records = records;
            this._largestRecords = new PriorityQueue<Record, ulong>();
            this._largestKeyPrefixes = new PriorityQueue<PrefixRecord, PrefixRecord>(PrefixRecord.Comparer);
            this._keyPrefix = new Dictionary<TypeKey, TypeKeyValue>(TypeKey.Comparer);
            this._typeDict = new Dictionary<string, TypeStatValue>();
            this._expiryDict = new Dictionary<string, ulong>();
        }

        public void Count()
        {
            Task.Factory.StartNew(() => 
            {
                while (!_records.IsAddingCompleted)
                {
                    try
                    {
                        if (_records.TryTake(out var item, 500))
                        {
                            this.CountLargestEntries(item, 500);
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
            });
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

        public Dictionary<string, ulong> GetExpiryInfo()
        {
            return _expiryDict;
        }

        private void CountExpiry(Record item)
        {
            if (item.Expiry > 0)
            {
                var sub = DateTimeOffset.FromUnixTimeMilliseconds(item.Expiry).Subtract(DateTimeOffset.UtcNow);

                // 0~1h, 1~3h, 3~12h, 12~24h, 24~72h, 72~168h, 168h~
                var hour = sub.TotalHours;

                if (hour < 1)
                {
                    InitOrAdd(_expiryDict, "0~1h", 1);
                }
                else if (hour >= 1 && hour < 3)
                {
                    InitOrAdd(_expiryDict, "0~3h", 1);
                }
                else if (hour >= 3 && hour < 12)
                {
                    InitOrAdd(_expiryDict, "3~12h", 1);
                }
                else if (hour >= 12 && hour < 24)
                {
                    InitOrAdd(_expiryDict, "12~24h", 1);
                }
                else if (hour >= 24 && hour < 72)
                {
                    InitOrAdd(_expiryDict, "1~3d", 1);
                }
                else if (hour >= 72 && hour < 168)
                {
                    InitOrAdd(_expiryDict, "3~7d", 1);
                }
                else if (hour >= 168)
                {
                    InitOrAdd(_expiryDict, ">7d", 1);
                }
            }
            else if (item.Expiry == 0)
            {
                InitOrAdd(_expiryDict, "None", 1);
            }
            else
            {
                if (_expiryDict.ContainsKey(item.Expiry.ToString()))
                {
                    _expiryDict[item.Expiry.ToString()]++;
                }
                else
                {
                    _expiryDict[item.Expiry.ToString()] = 1;
                }
            }
        }

        private void InitOrAdd(Dictionary<string, ulong> dict, string key, ulong val)
        {
            if (dict.ContainsKey(key))
            {
                dict[key] += val;
            }
            else
            {
                dict[key] = val;
            }
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
            if (this._typeDict.ContainsKey(record.Type))
            {
                this._typeDict[record.Type].Num++;
                this._typeDict[record.Type].Bytes += record.Bytes;
            }
            else
            {
                this._typeDict[record.Type] = new TypeStatValue
                {
                    Bytes = record.Bytes,
                    Num = 1,
                };
            }
        }
    }
}

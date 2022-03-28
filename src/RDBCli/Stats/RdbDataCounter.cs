using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace RDBCli
{
    internal class RdbDataCounter
    {
        public PriorityQueue<Record, ulong> LargestRecords = new PriorityQueue<Record, ulong>();

        public PriorityQueue<PrefixRecord, ulong> LargestKeyPrefixes { get; set; } = new PriorityQueue<PrefixRecord, ulong>();
        public Dictionary<TypeKey, ulong> KeyPrefixBytes = new Dictionary<TypeKey, ulong>(TypeKey.Comparer);
        public Dictionary<TypeKey, ulong> KeyPrefixNum = new Dictionary<TypeKey, ulong>(TypeKey.Comparer);

        public Dictionary<string, ulong> TypeBytes = new Dictionary<string, ulong>();
        public Dictionary<string, ulong> TypeNum = new Dictionary<string, ulong>();
        public char[] Separators { get; set; } = new char[] { ':', ';', ',', '_', '-' };

        private readonly BlockingCollection<Record> _records;

        public RdbDataCounter(BlockingCollection<Record> records)
        {
            this._records = records;
        }

        public void Count()
        {
            System.Threading.Tasks.Task.Factory.StartNew(() => 
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
            });
        }

        private void CountByKeyPrefix(Record record)
        {
            var prefixes = GetPrefixes(record.Key);

            var tKey = new TypeKey { Type = record.Type };

            foreach (var item in prefixes)
            {
                if (item.Length == 0) continue;

                tKey.Key = item;

                if (this.KeyPrefixNum.ContainsKey(tKey))
                {
                    this.KeyPrefixNum[tKey]++;
                }
                else
                {
                    this.KeyPrefixNum[tKey] = 1;
                }

                if (this.KeyPrefixBytes.ContainsKey(tKey))
                {
                    this.KeyPrefixBytes[tKey] += record.Bytes;
                }
                else
                {
                    this.KeyPrefixBytes[tKey] = record.Bytes;
                }
            }
        }

        private List<string> GetPrefixes(string s)
        {
            var res = new List<string>();

            var span = s.AsSpan();

            var sepIdx = span.IndexOfAny(Separators);

            if (sepIdx < 0)
            {
                res.Add(s);
            }

            if (sepIdx > -1)
            {
                var str = new string(span.Slice(0, sepIdx + 1));
               
                res.Add(str.TrimEnd(Separators));
            }

            return res;
        }

        private void CountLargestEntries(Record record, int num)
        {
            LargestRecords.Enqueue(record, record.Bytes);

            if (LargestRecords.Count > num)
            {
                _ = LargestRecords.Dequeue();
            }
        }

        private void CounteByType(Record record)
        {
            if (this.TypeNum.ContainsKey(record.Type))
            {
                this.TypeNum[record.Type]++;
            }
            else
            {
                this.TypeNum[record.Type] = 1;
            }

            if (this.TypeBytes.ContainsKey(record.Type))
            {
                this.TypeBytes[record.Type] += record.Bytes;
            }
            else
            {
                this.TypeBytes[record.Type] = record.Bytes;
            }
        }
    }
}

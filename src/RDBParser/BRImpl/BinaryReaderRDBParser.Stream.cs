using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadStream(BinaryReader br)
        {
            var listPacks = br.ReadLength();
            
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            _callback.StartStream(_key, (long)listPacks, _expiry, info);

            while (listPacks > 0)
            {
                var entityId = br.ReadStr();
                var data = br.ReadStr();
                _callback.StreamListPack(_key, entityId, data);

                listPacks--;
            }

            var items = br.ReadLength();
            var left = br.ReadLength();
            var right = br.ReadLength();
            var lastEntryId = $"{left}-{right}";

            var cgroups = br.ReadLength();
            var cgroupsData = new List<StreamGroup>();
            while (cgroups > 0)
            {
                var cgName = br.ReadStr();
                var l = br.ReadLength();
                var r = br.ReadLength();
                var lastCgEntryId = $"{l}-{r}";
                var pending = br.ReadLength();
                var groupPendingEntries = new List<StreamPendingEntry>();
                while (pending > 0)
                {
                    var eId = br.ReadBytes(16);
                    var delivery_time = br.ReadUInt64();
                    var delivery_count = br.ReadLength();
                    groupPendingEntries.Add(new StreamPendingEntry
                    {
                        Id = eId,
                        DeliveryTime = delivery_time,
                        DeliveryCount = delivery_count,
                    });

                    pending--;
                }
                var consumers = br.ReadLength();
                var consumersData = new List<StreamConsumerData>();
                while (consumers > 0)
                {
                    var cname = br.ReadStr();
                    var seenTime = br.ReadUInt64();
                    pending = br.ReadLength();
                    var consumerPendingEntries = new List<StreamConsumerPendingEntry>();
                    while (pending > 0)
                    {
                        var eId = br.ReadBytes(16);
                        consumerPendingEntries.Add(new StreamConsumerPendingEntry
                        {
                            Id = eId
                        });

                        pending--;
                    }

                    consumersData.Add(new StreamConsumerData
                    {
                        Name = cname,
                        SeenTime = seenTime,
                        Pending = consumerPendingEntries
                    });

                    consumers--;
                }

                cgroupsData.Add(new StreamGroup
                {
                    Name = cgName,
                    LastEntryId = lastCgEntryId,
                    Pending = groupPendingEntries,
                    Consumers = consumersData,
                });

                cgroups--;
            }

            _callback.EndStream(_key, items, lastEntryId, cgroupsData);
        }

        private void SkipStream(BinaryReader br)
        {
            var listPacks = br.ReadLength();

            while (listPacks > 0)
            {
                _ = br.ReadStr();
                _ = br.ReadStr();

                listPacks--;
            }

            _ = br.ReadLength();
            _ = br.ReadLength();
            _ = br.ReadLength();

            var cgroups = br.ReadLength();
            while (cgroups > 0)
            {
                _ = br.ReadStr();
                _ = br.ReadLength();
                _ = br.ReadLength();
                var pending = br.ReadLength();
                while (pending > 0)
                {
                    _ = br.ReadBytes(16);
                    _ = br.ReadBytes(8);
                    _ = br.ReadLength();

                    pending--;
                }
                var consumers = br.ReadLength();
                while (consumers > 0)
                {
                    br.SkipStr();
                    br.ReadBytes(8);
                    pending = br.ReadLength();
                    br.ReadBytes((int)(pending * 16));

                    consumers--;
                }

                cgroups--;
            }
        }
    }
}

using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadStream(BinaryReader br, byte[] key, int encType, long expiry, Info info)
        {
            var listPacks = br.ReadLength();

            _callback.StartStream(key, (long)listPacks, _expiry, info);

            while (listPacks > 0)
            {
                var entityId = br.ReadStr();
                var data = br.ReadStr();
                _callback.StreamListPack(key, entityId, data);

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
                var group_pending_entries = new List<StreamPendingEntry>();
                while (pending > 0)
                {
                    var eId = br.ReadBytes(16);
                    var delivery_time = br.ReadUInt64();
                    var delivery_count = br.ReadLength();
                    group_pending_entries.Add(new StreamPendingEntry
                    {
                        Id = eId,
                        DeliveryTime = delivery_time,
                        DeliveryCount = delivery_count,
                    });

                    pending--;
                }
                var consumers = br.ReadLength();
                var consumers_data = new List<StreamConsumerData>();
                while (consumers > 0)
                {
                    var cname = br.ReadStr();
                    var seenTime = br.ReadUInt64();
                    pending = br.ReadLength();
                    var consumer_pending_entries = new List<StreamConsumerPendingEntry>();
                    while (pending > 0)
                    {
                        var eId = br.ReadBytes(16);
                        consumer_pending_entries.Add(new StreamConsumerPendingEntry
                        {
                            Id = eId
                        });

                        pending--;
                    }

                    consumers_data.Add(new StreamConsumerData
                    {
                        Name = cname,
                        SeenTime = seenTime,
                        Pending = consumer_pending_entries
                    });

                    consumers--;
                }

                cgroupsData.Add(new StreamGroup
                {
                    Name = cgName,
                    LastEntryId = lastCgEntryId,
                    Pending = group_pending_entries,
                    Consumers = consumers_data,
                });

                cgroups--;
            }

            _callback.EndStream(key, items, lastEntryId, cgroupsData);
        }
    }
}

using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {
        private async Task SkipStreamAsync(PipeReader reader)
        {
            var listPacks = await reader.ReadLengthAsync();

            while (listPacks > 0)
            {
                _ = await reader.ReadStringAsync();
                _ = await reader.ReadStringAsync();

                listPacks--;
            }

            _ = await reader.ReadLengthAsync();
            _ = await reader.ReadLengthAsync();
            _ = await reader.ReadLengthAsync();

            var cgroups = await reader.ReadLengthAsync();
            while (cgroups > 0)
            {
                _ = await reader.ReadStringAsync();
                _ = await reader.ReadLengthAsync();
                _ = await reader.ReadLengthAsync();
                var pending = await reader.ReadLengthAsync();
                while (pending > 0)
                {
                    _ = await reader.ReadBytesAsync(16);
                    _ = await reader.ReadBytesAsync(8);
                    _ = await reader.ReadLengthAsync();

                    pending--;
                }
                var consumers = await reader.ReadLengthAsync();
                while (consumers > 0)
                {
                    await reader.SkipStringAsync();
                    await reader.ReadBytesAsync(8);
                    pending = await reader.ReadLengthAsync();
                    await reader.ReadBytesAsync((int)(pending * 16));

                    consumers--;
                }

                cgroups--;
            }
        }

        private async Task ReadStreamAsync(PipeReader reader)
        {
            var listPacks = await reader.ReadLengthAsync();

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            _callback.StartStream(_key, (long)listPacks, _expiry, info);

            while (listPacks > 0)
            {
                var entityId = await reader.ReadStringAsync();
                var data = await reader.ReadStringAsync();
                _callback.StreamListPack(_key, entityId, data);

                listPacks--;
            }

            var items = await reader.ReadLengthAsync();
            var left = await reader.ReadLengthAsync();
            var right = await reader.ReadLengthAsync();
            var lastEntryId = $"{left}-{right}";

            var cgroups = await reader.ReadLengthAsync();
            var cgroupsData = new List<StreamGroup>();
            while (cgroups > 0)
            {
                var cgName = await reader.ReadStringAsync();
                var l = await reader.ReadLengthAsync();
                var r = await reader.ReadLengthAsync();
                var lastCgEntryId = $"{l}-{r}";
                var pending = await reader.ReadLengthAsync();
                var group_pending_entries = new List<StreamPendingEntry>();
                while (pending > 0)
                {
                    var eId = await reader.ReadBytesAsync(16);
                    var timeBuff = await reader.ReadBytesAsync(8);
                    var delivery_time = (ulong)timeBuff.ReadInt64LittleEndianItem();
                    var delivery_count = await reader.ReadLengthAsync();
                    group_pending_entries.Add(new StreamPendingEntry
                    {
                        Id = eId.ToArray(),
                        DeliveryTime = delivery_time,
                        DeliveryCount = delivery_count,
                    });

                    pending--;
                }
                var consumers = await reader.ReadLengthAsync();
                var consumers_data = new List<StreamConsumerData>();
                while (consumers > 0)
                {
                    var cname = await reader.ReadStringAsync();
                    var timeBuff = await reader.ReadBytesAsync(8);
                    var seenTime = (ulong)timeBuff.ReadInt64LittleEndianItem();
                    pending = await reader.ReadLengthAsync();
                    var consumer_pending_entries = new List<StreamConsumerPendingEntry>();
                    while (pending > 0)
                    {
                        var eId = await reader.ReadBytesAsync(16);
                        consumer_pending_entries.Add(new StreamConsumerPendingEntry
                        {
                            Id = eId.ToArray()
                        });

                        pending--;

                    }

                    consumers_data.Add(new StreamConsumerData
                    {
                        Name = cname.ToArray(),
                        SeenTime = seenTime,
                        Pending = consumer_pending_entries
                    });

                    consumers--;
                }

                cgroupsData.Add(new StreamGroup
                {
                    Name = cgName.ToArray(),
                    LastEntryId = lastCgEntryId,
                    Pending = group_pending_entries,
                    Consumers = consumers_data,
                });

                cgroups--;
            }

            _callback.EndStream(_key, items, lastEntryId, cgroupsData);
        }
    }
}

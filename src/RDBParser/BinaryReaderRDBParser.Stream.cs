using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadStream(BinaryReader br, int encType)
        {
            // https://github.com/redis/redis/blob/7.0-rc3/src/rdb.c#L2329-L2613
            var listPacks = br.ReadLength();
            
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            _callback.StartStream(_key, (long)listPacks, _expiry, info);

            while (listPacks > 0)
            {
                // the master ID
                var entityId = br.ReadStr();
                var data = br.ReadStr();
                _callback.StreamListPack(_key, entityId, data);

                listPacks--;
            }

            // total number of items inside the stream
            var items = br.ReadLength();

            // the last entry ID
            var lastMs = br.ReadLength();
            var lastSeq = br.ReadLength();
            var lastEntryId = $"{lastMs}-{lastSeq}";

            var firstEntryId = "0-0";
            var maxDeletedEntryId = "0-0";
            ulong entriesAdded = items;
            if (encType == Constant.DataType.STREAM_LISTPACKS_2)
            {
                // the first entry ID
                var firstMs = br.ReadLength();
                var firstSeq = br.ReadLength();
                firstEntryId = $"{firstMs}-{firstSeq}";

                // the maximal deleted entry ID
                var maxDeletedEntryIdMs = br.ReadLength();
                var maxDeletedEntryIdSeq = br.ReadLength();
                maxDeletedEntryId = $"{maxDeletedEntryIdMs}-{maxDeletedEntryIdSeq}";

                // the offset. 
                entriesAdded = br.ReadLength();
            }

            var cgroups = br.ReadLength();
            var cgroupsData = new List<StreamCGEntity>();
            while (cgroups > 0)
            {
                var cgName = br.ReadStr();
                var l = br.ReadLength();
                var r = br.ReadLength();
                var lastCgEntryId = $"{l}-{r}";

                // group offset
                ulong cgOffset = 0;
                if (encType == Constant.DataType.STREAM_LISTPACKS_2)
                {
                    cgOffset = br.ReadLength();
                }

                // the global PEL for this consumer group
                var pelSize = br.ReadLength();
                var groupPendingEntries = new List<StreamPendingEntity>();
                while (pelSize > 0)
                {
                    // nack
                    var eId = br.ReadBytes(16);
                    var deliveryTime = br.ReadUInt64();
                    var deliveryCount = br.ReadLength();
                    groupPendingEntries.Add(new StreamPendingEntity
                    {
                        Id = eId,
                        DeliveryTime = deliveryTime,
                        DeliveryCount = deliveryCount,
                    });

                    pelSize--;
                }

                // the consumers and their local PELs
                var consumers = br.ReadLength();
                var consumersData = new List<StreamConsumerEntity>();
                while (consumers > 0)
                {
                    var cname = br.ReadStr();
                    var seenTime = br.ReadUInt64();

                    // the PEL about entries owned by this specific consumer
                    pelSize = br.ReadLength();
                    var consumerPendingEntries = new List<StreamConsumerPendingEntity>();
                    while (pelSize > 0)
                    {
                        // streamID
                        var eId = br.ReadBytes(16);
                        consumerPendingEntries.Add(new StreamConsumerPendingEntity
                        {
                            Id = eId
                        });

                        pelSize--;
                    }

                    consumersData.Add(new StreamConsumerEntity
                    {
                        Name = cname,
                        SeenTime = seenTime,
                        Pending = consumerPendingEntries
                    });

                    consumers--;
                }

                cgroupsData.Add(new StreamCGEntity
                {
                    Name = cgName,
                    LastEntryId = lastCgEntryId,
                    EntriesRead = cgOffset,
                    Pending = groupPendingEntries,
                    Consumers = consumersData,
                });

                cgroups--;
            }

            var entity = new StreamEntity
            {
                Length = items,
                LastId = lastEntryId,
                FirstId = firstEntryId,
                MaxDeletedEntryId = maxDeletedEntryId,
                EntriesAdded = entriesAdded,
                CGroups = cgroupsData
            };

            _callback.EndStream(_key, entity);
        }

        private void SkipStream(BinaryReader br, int encType)
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

            if (encType == Constant.DataType.STREAM_LISTPACKS_2)
            {
                // the first entry ID
                _ = br.ReadLength();
                _ = br.ReadLength();

                // the maximal deleted entry ID
                _ = br.ReadLength();
                _ = br.ReadLength();

                // the offset. 
                _ = br.ReadLength();
            }

            var cgroups = br.ReadLength();
            while (cgroups > 0)
            {
                _ = br.ReadStr();
                _ = br.ReadLength();
                _ = br.ReadLength();

                if (encType == Constant.DataType.STREAM_LISTPACKS_2)
                {
                    _ = br.ReadLength();
                }

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

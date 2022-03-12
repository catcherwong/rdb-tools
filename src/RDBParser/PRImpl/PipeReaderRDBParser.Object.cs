using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {        
        private async Task ReadObjectAsync(PipeReader reader, ReadOnlySequence<byte> key, int encType, long expiry, Info info)
        {
            if (encType == Constant.DataType.STRING)
            {
                var value = await reader.ReadStringAsync();

                info.Encoding = "string";
                _callback.Set(key, value, expiry, info);
            }
            else if (encType == Constant.DataType.LIST)
            {
                var length = await reader.ReadLengthAsync();
                info.Encoding = "linkedlist";
                _callback.StartList(key, expiry, info);
                while (length > 0)
                {
                    length--;
                    var val = await reader.ReadStringAsync();
                    _callback.RPush(key, val);
                }
                _callback.EndList(key, info);
            }
            else if (encType == Constant.DataType.SET)
            {
                var cardinality = await reader.ReadLengthAsync();
                info.Encoding = "hashtable";
                _callback.StartSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = await reader.ReadStringAsync();
                    _callback.SAdd(key, member);
                }
                _callback.EndSet(key);
            }
            else if (encType == Constant.DataType.ZSET || encType == Constant.DataType.ZSET_2)
            {
                var cardinality = await reader.ReadLengthAsync();
                info.Encoding = "skiplist";
                _callback.StartSortedSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = await reader.ReadStringAsync();

                    double score = encType == Constant.DataType.ZSET_2
                       ? await reader.ReadDoubleAsync()
                       : await reader.ReadFloatAsync();

                    _callback.ZAdd(key, score, member);
                }
                _callback.EndSortedSet(key);
            }
            else if (encType == Constant.DataType.HASH)
            {
                var length = await reader.ReadLengthAsync();

                info.Encoding = "hashtable";
                _callback.StartHash(key, (long)length, expiry, info);

                while (length > 0)
                {
                    length--;
                    var field = await reader.ReadStringAsync();
                    var value = await reader.ReadStringAsync();

                    _callback.HSet(key, field, value);
                }
                _callback.EndHash(key);
            }
            else if (encType == Constant.DataType.HASH_ZIPMAP)
            {
                await ReadZipMapAsync(reader, key, expiry, info);
            }
            else if (encType == Constant.DataType.LIST_ZIPLIST)
            {
                await ReadZipListAsync(reader, key, expiry, info);
            }
            else if (encType == Constant.DataType.SET_INTSET)
            {
                await ReadIntSetAsync(reader, key, expiry, info);
            }
            else if (encType == Constant.DataType.ZSET_ZIPLIST)
            {
                await ReadZSetFromZiplistAsync(reader, key, expiry, info);
            }
            else if (encType == Constant.DataType.HASH_ZIPLIST)
            {
                await ReadHashFromZiplistAsync(reader, key, expiry, info);
            }
            else if (encType == Constant.DataType.LIST_QUICKLIST)
            {
                await ReadListFromQuickListAsync(reader, key, expiry, info);
            }
            else if (encType == Constant.DataType.MODULE)
            {
                throw new RDBParserException($"Unable to read Redis Modules RDB objects (key {key})");
            }
            else if (encType == Constant.DataType.MODULE_2)
            {
                await reader.SkipModuleAsync();
            }
            else if (encType == Constant.DataType.STREAM_LISTPACKS)
            {
                await reader.SkipStreamAsync();
            }
            else
            {
                throw new RDBParserException($"Invalid object type {encType} for {key} ");
            }
        }

        private async Task ReadIntSetAsync(PipeReader reader, ReadOnlySequence<byte> key, long expiry, Info info)
        {
            var raw = await reader.ReadStringAsync();
            var rd = PipeReader.Create(raw);

            var encodingBuff = await rd.ReadBytesAsync(4);
            var encoding = BinaryPrimitives.ReadUInt32LittleEndian(encodingBuff.FirstSpan);

            var numEntriesBuff = await rd.ReadBytesAsync(2);
            var numEntries = BinaryPrimitives.ReadUInt32LittleEndian(numEntriesBuff.FirstSpan);

            info.Encoding = "intset";
            info.SizeOfValue = (int)raw.Length;
            _callback.StartList(key, expiry, info);

            info.Encoding = "intset";
            info.SizeOfValue = (int)raw.Length;
            _callback.StartList(key, expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                if (encoding != 8 & encoding == 4 & encoding == 2)
                    throw new RDBParserException($"Invalid encoding {encoding} for key {key}");

                var entry = await rd.ReadBytesAsync((int)encoding);
                _callback.SAdd(key, entry);
            }

            _callback.EndSet(key);
        }

        private async Task ReadListFromQuickListAsync(PipeReader reader, ReadOnlySequence<byte> key, long expiry, Info info)
        {
            var length = await reader.ReadLengthAsync();
            var totalSize = 0;
            info.Encoding = "quicklist";
            info.Zips = length;
            _callback.StartList(key, expiry, info);

            while (length > 0)
            {
                length--;

                var rawString = await reader.ReadStringAsync();
                totalSize += (int)rawString.Length;

                var rd = PipeReader.Create(rawString);

                var zlbytes = await reader.ReadBytesAsync(4);
                var tail_offset = await reader.ReadBytesAsync(4);

                var numEntriesBuff = await rd.ReadBytesAsync(2);
                var numEntries = BinaryPrimitives.ReadUInt32LittleEndian(numEntriesBuff.FirstSpan);

                for (int i = 0; i < numEntries; i++)
                {
                    _callback.RPush(key, await ReadZipListEntryAsync(rd));
                }

                var zlistEnd = await rd.ReadSingleByteAsync();
                if (zlistEnd != 255)
                {
                    throw new RDBParserException("Invalid zip list end");
                }
            }

            _callback.EndList(key, info);
        }
    }
}

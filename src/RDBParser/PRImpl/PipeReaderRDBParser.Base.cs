using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {        
        private async Task ReadObjectAsync(PipeReader reader, int encType)
        {
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;

            if (encType == Constant.DataType.STRING)
            {
                var value = await reader.ReadStringAsync();              
                info.Encoding = "string";
                _callback.Set(_key, value, _expiry, info);
            }
            else if (encType == Constant.DataType.LIST)
            {
                var length = await reader.ReadLengthAsync();
                info.Encoding = "linkedlist";
                _callback.StartList(_key, _expiry, info);
                while (length > 0)
                {
                    length--;
                    var val = await reader.ReadStringAsync();
                    _callback.RPush(_key, val);
                }
                _callback.EndList(_key, info);
            }
            else if (encType == Constant.DataType.SET)
            {
                var cardinality = await reader.ReadLengthAsync();
                info.Encoding = "hashtable";
                _callback.StartSet(_key, (long)cardinality, _expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = await reader.ReadStringAsync();
                    _callback.SAdd(_key, member);
                }
                _callback.EndSet(_key);
            }
            else if (encType == Constant.DataType.ZSET || encType == Constant.DataType.ZSET_2)
            {
                var cardinality = await reader.ReadLengthAsync();
                info.Encoding = "skiplist";
                _callback.StartSortedSet(_key, (long)cardinality, _expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = await reader.ReadStringAsync();

                    double score = encType == Constant.DataType.ZSET_2
                       ? await reader.ReadDoubleAsync()
                       : await reader.ReadFloatAsync();

                    _callback.ZAdd(_key, score, member);
                }
                _callback.EndSortedSet(_key);
            }
            else if (encType == Constant.DataType.HASH)
            {
                var length = await reader.ReadLengthAsync();

                info.Encoding = "hashtable";
                _callback.StartHash(_key, (long)length, _expiry, info);

                while (length > 0)
                {
                    length--;
                    var field = await reader.ReadStringAsync();
                    var value = await reader.ReadStringAsync();

                    _callback.HSet(_key, field, value);
                }
                _callback.EndHash(_key);
            }
            else if (encType == Constant.DataType.HASH_ZIPMAP)
            {
                await ReadZipMapAsync(reader, info);
            }
            else if (encType == Constant.DataType.LIST_ZIPLIST)
            {
                await ReadZipListAsync(reader, info);
            }
            else if (encType == Constant.DataType.SET_INTSET)
            {
                await ReadIntSetAsync(reader, info);
            }
            else if (encType == Constant.DataType.ZSET_ZIPLIST)
            {
                await ReadZSetFromZiplistAsync(reader, info);
            }
            else if (encType == Constant.DataType.HASH_ZIPLIST)
            {
                await ReadHashFromZiplistAsync(reader, info);
            }
            else if (encType == Constant.DataType.LIST_QUICKLIST)
            {
                await ReadListFromQuickListAsync(reader, info);
            }
            else if (encType == Constant.DataType.MODULE)
            {
                throw new RDBParserException($"Unable to read Redis Modules RDB objects (key {_key})");
            }
            else if (encType == Constant.DataType.MODULE_2)
            {
                await SkipModuleAsync(reader);
            }
            else if (encType == Constant.DataType.STREAM_LISTPACKS)
            {
                await SkipStreamAsync(reader);
            }
            else
            {
                throw new RDBParserException($"Invalid object type {encType} for {_key} ");
            }
        }
       
        private async Task ReadListFromQuickListAsync(PipeReader reader, Info info)
        {
            var length = await reader.ReadLengthAsync();
            var totalSize = 0;
            info.Encoding = "quicklist";
            info.Zips = length;
            _callback.StartList(_key, _expiry, info);

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
                    _callback.RPush(_key, await ReadZipListEntryAsync(rd));
                }

                var zlistEnd = await rd.ReadSingleByteAsync();
                if (zlistEnd != 255)
                {
                    throw new RDBParserException("Invalid zip list end");
                }
            }

            _callback.EndList(_key, info);
        }
    }
}

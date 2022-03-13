using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {
        private async Task ReadZipListAsync(PipeReader reader, Info info)
        {
            var ziplist = await reader.ReadStringAsync();

            var rd = PipeReader.Create(ziplist);

            var zlBuff = await rd.ReadBytesAsync(4);
            var zlbytes = BinaryPrimitives.ReadUInt32LittleEndian(zlBuff.FirstSpan);

            var tail_offsetBuff = await rd.ReadBytesAsync(4);
            var tail_offset = BinaryPrimitives.ReadUInt32LittleEndian(tail_offsetBuff.FirstSpan);

            var numEntriesBuff = await rd.ReadBytesAsync(2);
            var numEntries = BinaryPrimitives.ReadUInt32LittleEndian(numEntriesBuff.FirstSpan);

            info.Encoding = "ziplist";
            info.SizeOfValue = (int)ziplist.Length;
            _callback.StartList(_key, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var val = await ReadZipListEntryAsync(rd);
                _callback.RPush(_key, val);
            }

            var zlistEnd = await rd.ReadSingleByteAsync();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {_key}");

            _callback.EndList(_key, info);
        }

        private async Task<ReadOnlySequence<byte>> ReadZipListEntryAsync(PipeReader reader)
        {
            var length = 0;
            ReadOnlySequence<byte> value = default;
            var prevLength = await reader.ReadSingleByteAsync();
            
            if (prevLength == 254) _ = await reader.ReadBytesAsync(4);

            var entryHeader = await reader.ReadSingleByteAsync();

            if(entryHeader >> 6 == 0)
            {
                length = entryHeader & 0x3F;
                value = await reader.ReadBytesAsync(length);
            }
            else if(entryHeader >> 6 == 1)
            {
                length = (entryHeader & 0x3F) << 8 | await reader.ReadSingleByteAsync();
                value = await reader.ReadBytesAsync(length);
            }
             else if(entryHeader >> 6 == 2)
            {
                var d = await reader.ReadBytesAsync(4);
                length = (int)BinaryPrimitives.ReadUInt32BigEndian(d.FirstSpan);
                value = await reader.ReadBytesAsync(length);
            }
            else if (entryHeader >> 4 == 12)
            {
                value = await reader.ReadBytesAsync(2);
            }
            else if (entryHeader >> 4 == 13)
            {
                value = await reader.ReadBytesAsync(4);
            }
            else if (entryHeader >> 4 == 14)
            {
                value = await reader.ReadBytesAsync(8);
            }
            else if (entryHeader == 240)
            {
                var bytes = new byte[4];
                bytes[1] = await reader.ReadSingleByteAsync();
                bytes[2] = await reader.ReadSingleByteAsync();
                bytes[3] = await reader.ReadSingleByteAsync();
                
                return new ReadOnlySequence<byte>(bytes);
            }
            else if (entryHeader == 254)
            {
                value = await reader.ReadBytesAsync(1);
            }
            else if (entryHeader >= 241 && entryHeader <= 253)
            {
                var b = new byte[]{(byte)(entryHeader - 241)};
                value = new ReadOnlySequence<byte>(b);
            }

            return value;
        }
    
        private async Task ReadHashFromZiplistAsync(PipeReader reader, Info info)
        {
            var raw = await reader.ReadStringAsync();
            
            var rd = PipeReader.Create(raw);

            var zlBuff = await rd.ReadBytesAsync(4);
            var zlbytes = BinaryPrimitives.ReadUInt32LittleEndian(zlBuff.FirstSpan);

            var tail_offsetBuff = await rd.ReadBytesAsync(4);
            var tail_offset = BinaryPrimitives.ReadUInt32LittleEndian(tail_offsetBuff.FirstSpan);

            var numEntriesBuff = await rd.ReadBytesAsync(2);
            var numEntries = BinaryPrimitives.ReadUInt32LittleEndian(numEntriesBuff.FirstSpan);

            if (numEntries % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEntries} for key {_key}");

            numEntries = (ushort)(numEntries / 2);

            info.Encoding = "ziplist";
            info.SizeOfValue = (int)raw.Length;
            _callback.StartHash(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var field = await ReadZipListEntryAsync(rd);
                var value = await ReadZipListEntryAsync(rd);
                _callback.HSet(_key, field, value);
            }

            var zlistEnd = await rd.ReadSingleByteAsync();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {_key}");

            _callback.EndHash(_key);
        }

        private async Task ReadZSetFromZiplistAsync(PipeReader reader, Info info)
        {
            var raw = await reader.ReadStringAsync();
            var rd = PipeReader.Create(raw);

            var zlBuff = await rd.ReadBytesAsync(4);
            var zlbytes = BinaryPrimitives.ReadUInt32LittleEndian(zlBuff.FirstSpan);

            var tail_offsetBuff = await rd.ReadBytesAsync(4);
            var tail_offset = BinaryPrimitives.ReadUInt32LittleEndian(tail_offsetBuff.FirstSpan);

            var numEntriesBuff = await rd.ReadBytesAsync(2);
            var numEntries = BinaryPrimitives.ReadUInt32LittleEndian(numEntriesBuff.FirstSpan);

            if (numEntries % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEntries} for key {_key}");

            numEntries = (ushort)(numEntries / 2);

            _callback.StartSortedSet(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var member = await ReadZipListEntryAsync(rd);
                var score = await ReadZipListEntryAsync(rd);

                var str = EncodingExtensions.GetString(Encoding.UTF8, score);
                double.TryParse(str, out var realScore);
                _callback.ZAdd(_key, realScore, member);
            }

            var zlistEnd = await rd.ReadSingleByteAsync();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {_key}");

            _callback.EndSortedSet(_key);
        }
    }
}

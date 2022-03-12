using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser
    {
        private void ReadZipList(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tail_offset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            info.Encoding = "ziplist";
            info.SizeOfValue = raw.Length;
            _callback.StartList(key, expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var val = ReadZipListEntry(rd);
                _callback.RPush(key, val);
            }

            var zlistEnd = rd.ReadByte();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {key}");

            _callback.EndList(key, info);
        }

        private byte[] ReadZipListEntry(BinaryReader br)
        {
            var length = 0;
            byte[] value = null;

            var prevLength = br.ReadByte();

            if (prevLength == 254) _ = br.ReadUInt32();

            var entryHeader = br.ReadByte();
            if (entryHeader >> 6 == 0)
            {
                length = entryHeader & 0x3F;
                value = br.ReadBytes(length);
            }
            else if (entryHeader >> 6 == 1)
            {
                length = (entryHeader & 0x3F) << 8 | br.ReadByte();
                value = br.ReadBytes(length);
            }
            else if (entryHeader >> 6 == 2)
            {
                length = (int)br.ReadUInt32BigEndian();
                value = br.ReadBytes(length);
            }
            else if (entryHeader >> 4 == 12)
            {
                value = br.ReadBytes(2);
            }
            else if (entryHeader >> 4 == 13)
            {
                value = br.ReadBytes(4);
            }
            else if (entryHeader >> 4 == 14)
            {
                value = br.ReadBytes(8);
            }
            else if (entryHeader == 240)
            {
                var bytes = new byte[4];
                bytes[1] = br.ReadByte();
                bytes[2] = br.ReadByte();
                bytes[3] = br.ReadByte();
                return bytes;
            }
            else if (entryHeader == 254)
            {
                value = br.ReadBytes(1);
            }
            else if (entryHeader >= 241 && entryHeader <= 253)
            {
                value = new byte[1] { (byte)(entryHeader - 241) };
            }

            return value;
        }

        private void ReadHashFromZiplist(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tail_offset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            if (numEntries % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEntries} for key {key}");

            numEntries = (ushort)(numEntries / 2);

            info.Encoding = "ziplist";
            info.SizeOfValue = raw.Length;
            _callback.StartHash(key, numEntries, expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var field = ReadZipListEntry(rd);
                var value = ReadZipListEntry(rd);
                _callback.HSet(key, field, value);
            }

            var zlistEnd = rd.ReadByte();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {key}");

            _callback.EndHash(key);
        }

        private void ReadZSetFromZiplist(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tail_offset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            if (numEntries % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEntries} for key {key}");

            numEntries = (ushort)(numEntries / 2);

            info.Encoding = "ziplist";
            info.SizeOfValue = raw.Length;
            _callback.StartSortedSet(key, numEntries, expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var member = ReadZipListEntry(rd);
                var score = ReadZipListEntry(rd);

                var str = System.Text.Encoding.UTF8.GetString(score);
                double.TryParse(str, out var realScore);
                _callback.ZAdd(key, realScore, member);
            }

            var zlistEnd = rd.ReadByte();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {key}");

            _callback.EndSortedSet(key);
        }
    }
}

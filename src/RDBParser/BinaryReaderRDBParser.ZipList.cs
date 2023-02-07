using System;
using System.IO;
using System.Text;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser
    {
        private void ReadZipList(BinaryReader br)
        {
            // https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#ziplist-encoding
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tail_offset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = Constant.ObjEncoding.ZIPLIST;
            info.SizeOfValue = raw.Length;
            _callback.StartList(_key, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var val = ReadZipListEntry(rd);
                _callback.RPush(_key, val);
            }

            var zlistEnd = rd.ReadByte();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {_key}");

            _callback.EndList(_key, info);
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
                length = ((entryHeader & 0x3F) << 8) | br.ReadByte();
                value = br.ReadBytes(length);
            }
            else if (entryHeader >> 6 == 2)
            {
                length = (int)br.ReadUInt32BigEndian();
                value = br.ReadBytes(length);
            }
            else if (entryHeader >> 4 == 12)
            {
                // short
                var tmp = br.ReadBytes(2);
                value = Encoding.UTF8.GetBytes(BitConverter.ToInt16(tmp).ToString());
            }
            else if (entryHeader >> 4 == 13)
            {
                // int
                var tmp = br.ReadBytes(4);
                value = Encoding.UTF8.GetBytes(BitConverter.ToInt32(tmp).ToString());
            }
            else if (entryHeader >> 4 == 14)
            {
                // long
                var tmp = br.ReadBytes(8);
                value = Encoding.UTF8.GetBytes(BitConverter.ToInt64(tmp).ToString());
            }
            else if (entryHeader == 240)
            {
                // 24 bit int
                var tmp = new byte[4];
                tmp[0] = 0;
                tmp[1] = br.ReadByte();
                tmp[2] = br.ReadByte();
                tmp[3] = br.ReadByte();

                return Encoding.UTF8.GetBytes((BitConverter.ToInt32(tmp) >> 8).ToString());
            }
            else if (entryHeader == 254)
            {
                var tmp = br.ReadBytes(1);
                value = Encoding.UTF8.GetBytes(((sbyte)tmp[0]).ToString());
            }
            else if (entryHeader >= 241 && entryHeader <= 253)
            {
                var tmp = new byte[1] { (byte)(entryHeader - 241) };
                value = Encoding.UTF8.GetBytes(((sbyte)tmp[0]).ToString());
            }

            return value;
        }

        private void ReadHashFromZiplist(BinaryReader br)
        {
            // https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#hashmap-in-ziplist-encoding
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tailOffset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            if (numEntries % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEntries} for key {_key}");

            numEntries = (ushort)(numEntries / 2);
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = Constant.ObjEncoding.ZIPLIST;
            info.SizeOfValue = raw.Length;
            _callback.StartHash(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var field = ReadZipListEntry(rd);
                var value = ReadZipListEntry(rd);
                _callback.HSet(_key, field, value);
            }

            var zlistEnd = rd.ReadByte();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {_key}");

            _callback.EndHash(_key);
        }

        private void ReadZSetFromZiplist(BinaryReader br)
        {
            // https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#sorted-set-as-ziplist-encoding
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tailOffset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            if (numEntries % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEntries} for key {_key}");

            numEntries = (ushort)(numEntries / 2);
            
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = Constant.ObjEncoding.ZIPLIST;
            info.SizeOfValue = raw.Length;
            _callback.StartSortedSet(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var member = ReadZipListEntry(rd);
                var score = ReadZipListEntry(rd);

                double realScore = 0d;
                var str = System.Text.Encoding.UTF8.GetString(score);
                if (!double.TryParse(str, out realScore))
                {
                    if (score.Length == 2)
                    {
                        // 16 bit handle here
                        realScore = System.BitConverter.ToInt16(score, 0);
                    }
                    else if (score.Length == 1)
                    {
                        // 8 bit handle here
                        _ = double.TryParse(System.BitConverter.ToString(score), out realScore);
                    }
                }

                _callback.ZAdd(_key, realScore, member);
            }

            var zlistEnd = rd.ReadByte();
            if (zlistEnd != 255) throw new RDBParserException($"Invalid zip list end - {zlistEnd} for key {_key}");

            _callback.EndSortedSet(_key);
        }
    }
}

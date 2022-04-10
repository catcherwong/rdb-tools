using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser
    {
        private (byte[] encoding, byte[] data, long len) ReadListPackEntry(BinaryReader rd)
        {
            ulong count;

            var b = rd.ReadByte();
            if ((b & 0x80) == 0)
            {
                var encoding = new byte[] { b };
                var backlen = rd.ReadBytes(1);

                return (encoding, encoding, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xC0) == 0x80)
            {
                count = ((ulong)b & 0x3f);

                var encoding = new byte[] { b };
                var data = rd.ReadBytes((int)count);
                var backlen = rd.ReadBytes((int)lpEncodeBacklen(1 + count));

                return (encoding, data, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xE0) == 0xC0)
            {
                var encoding = new byte[] { b, rd.ReadByte() };
                var backlen = rd.ReadBytes(1);

                return (encoding, encoding, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xFF) == 0xF1)
            {
                var encoding = new byte[] { b, rd.ReadByte(), rd.ReadByte() };
                var backlen = rd.ReadBytes(1);

                return (encoding, encoding, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xFF) == 0xF2)
            {
                var p1 = rd.ReadByte();
                var p2 = rd.ReadByte();
                var p3 = rd.ReadByte();

                var encoding = new byte[] { b, p1, p2, p3 };
                var backlen = rd.ReadBytes(1);

                return (encoding, encoding, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xFF) == 0xF3)
            {
                var p1 = rd.ReadByte();
                var p2 = rd.ReadByte();
                var p3 = rd.ReadByte();
                var p4 = rd.ReadByte();

                var encoding = new byte[] { b, p1, p2, p3, p4 };
                var backlen = rd.ReadBytes(1);

                return (encoding, encoding, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xFF) == 0xF4)
            {
                var p1 = rd.ReadByte();
                var p2 = rd.ReadByte();
                var p3 = rd.ReadByte();
                var p4 = rd.ReadByte();
                var p5 = rd.ReadByte();
                var p6 = rd.ReadByte();
                var p7 = rd.ReadByte();
                var p8 = rd.ReadByte();

                var encoding = new byte[] { b, p1, p2, p3, p4, p5, p6, p7, p8 };
                var backlen = rd.ReadBytes(1);

                return (encoding, encoding, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xF0) == 0xE0)
            {
                var encoding = new byte[] { b, rd.ReadByte() };

                var p1 = encoding[1];
                count = ((ulong)b & 0xF) << 8 | p1;
                var data = rd.ReadBytes((int)count);
                var backlen = rd.ReadBytes((int)lpEncodeBacklen(2 + count));

                return (encoding, data, lpDecodeBacklen(backlen));
            }
            else if ((b & 0xFF) == 0xF0)
            {
                var p1 = rd.ReadByte();
                var p2 = rd.ReadByte();
                var p3 = rd.ReadByte();
                var p4 = rd.ReadByte();

                var encoding = new byte[] { b, p1, p2, p3, p4 };

                count = (ulong)p1 << 0 | (ulong)p2 << 8 | (ulong)p3 << 16 | (ulong)p4 << 24;
                var data = rd.ReadBytes((int)count);

                var backlen = rd.ReadBytes((int)lpEncodeBacklen(5 + count));
                return (encoding, data, lpDecodeBacklen(backlen));
            }
            else
            {
                throw new RDBParserException("");
            }
        }

        private void ReadHashFromListPack(BinaryReader br)
        {
            // <total_bytes><size><entry><entry>..<entry><end>
            var rawString = br.ReadStr();
            using MemoryStream stream = new MemoryStream(rawString);
            using var rd = new BinaryReader(stream);

            // <total_bytes>
            var bytes = lpGetTotalBytes(rd);
            // <size>
            var numEle = lpGetNumElements(rd);
            if (numEle % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEle} for key {_key}");

            var numEntries = (ushort)(numEle / 2);

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = "listpack";
            info.SizeOfValue = rawString.Length;
            _callback.StartHash(_key, numEntries, _expiry, info);

            // <entry>
            for (int i = 0; i < numEntries; i++)
            {
                // <encode><val><backlen>
                var field = ReadListPackEntry(rd);
                var value = ReadListPackEntry(rd);
                _callback.HSet(_key, field.data, value.data);
            }

            var lpEnd = rd.ReadByte();
            if (lpEnd != 0xFF) throw new RDBParserException($"Invalid list pack end - {lpEnd} for key {_key}");

            _callback.EndHash(_key);
        }

        private void ReadZSetFromListPack(BinaryReader br)
        {
            // <total_bytes><size><entry><entry>..<entry><end>
            var rawString = br.ReadStr();
            using MemoryStream stream = new MemoryStream(rawString);
            using var rd = new BinaryReader(stream);

            // <total_bytes>
            var bytes = lpGetTotalBytes(rd);
            // <size>
            var numEle = lpGetNumElements(rd);
            if (numEle % 2 != 0) throw new RDBParserException($"Expected even number of elements, but found {numEle} for key {_key}");

            var numEntries = (ushort)(numEle / 2);

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = "listpack";
            info.SizeOfValue = rawString.Length;
            _callback.StartSortedSet(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                var member = ReadListPackEntry(rd);
                var score = ReadListPackEntry(rd);

                double realScore = 0d;
                var str = System.Text.Encoding.UTF8.GetString(score.data);
                if (!double.TryParse(str, out realScore))
                {
                    realScore = RedisRdbObjectHelper.LpConvertBytesToInt64(score.data);
                }

                _callback.ZAdd(_key, realScore, member.data);
            }

            var lpEnd = rd.ReadByte();
            if (lpEnd != 0xFF) throw new RDBParserException($"Invalid list pack end - {lpEnd} for key {_key}");

            _callback.EndSortedSet(_key);
        }

        private uint lpGetTotalBytes(BinaryReader br)
        {
            return (uint)br.ReadByte() << 0 |
                    (uint)br.ReadByte() << 8 |
                    (uint)br.ReadByte() << 16 |
                    (uint)br.ReadByte() << 24;
        }

        private uint lpGetNumElements(BinaryReader br)
        {
            return (uint)br.ReadByte() << 0 |
                    (uint)br.ReadByte() << 8;
        }

        private ulong lpEncodeBacklen(ulong l)
        {
            if (l <= 127) return 1;
            else if (l < 16383) return 2;
            else if (l < 2097151) return 3;
            else if (l < 268435455) return 4;
            else return 5;
        }

        private long lpDecodeBacklen(byte[] p)
        {
            return 0;
        }
    }
}

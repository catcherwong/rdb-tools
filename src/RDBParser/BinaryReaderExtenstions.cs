using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    internal static class BinaryReaderExtenstions
    {
        public static ulong ReadLength(this BinaryReader br)
        { 
            var (len, _) = ReadLengthWithEncoding(br);
            return len;
        }

        public static (ulong Length, bool IsEncoded) ReadLengthWithEncoding(this BinaryReader br)
        {
            ulong len = 0;
            var isEncoded = false;

            var b = br.ReadByte();
            var encType = (b & 0xC0) >> 6;

            if (encType == Constant.LengthEncoding.BIT6)
            {
                len = (ulong)(b & 0x3F);
            }
            else if (encType == Constant.LengthEncoding.BIT14)
            {
                var b1 = br.ReadByte();
                len = (ulong)((b & 0x3F) << 8 | b1);
            }
            else if (encType == Constant.LengthEncoding.ENCVAL)
            {
                len = (ulong)(b & 0x3F);
                isEncoded = true;
            }
            else if (b == Constant.LengthEncoding.BIT32)
            {
                len = br.ReadUInt32BigEndian();
            }
            else if (b == Constant.LengthEncoding.BIT64)
            {
                len = br.ReadInt64BigEndian();
            }
            else
            {
                throw new RDBParserException($"Invalid string encoding {encType} (encoding byte {b})");
            }

            return (len, isEncoded);
        }

        public static byte[] ReadStr(this BinaryReader br)
        {
            var (length, isEncoded) = ReadLengthWithEncoding(br);

            if (!isEncoded) return br.ReadBytes((int)length);

            if (length == Constant.EncType.INT8)
            {
                return br.ReadBytes(1);
            }
            else if (length == Constant.EncType.INT16)
            {
                return br.ReadBytes(2);
            }
            else if (length == Constant.EncType.INT32)
            {
                return br.ReadBytes(4);
            }
            else if (length == Constant.EncType.LZF)
            {
                var clen = ReadLength(br);
                var ulen = ReadLength(br);

                var compressed = br.ReadBytes((int)clen);
                var decompressed = LzfDecompress(compressed, (int)ulen);

                if (decompressed.Length != (int)ulen)
                    throw new RDBParserException($"decompressed string length {decompressed.Length} didn't match expected length {(int)ulen}");

                return decompressed;
            }
            else
            {
                throw new RDBParserException($"Invalid string encoding {length}");
            }
        }

        private static byte[] LzfDecompress(byte[] compressed, int ulen)
        {
            var outStream = new List<byte>(ulen);
            var outIndex = 0;

            var inLen = compressed.Length;
            var inIndex = 0;

            while (inIndex < inLen)
            {
                var ctrl = compressed[inIndex];

                inIndex = inIndex + 1;

                if (ctrl < 32)
                {
                    for (int i = 0; i < ctrl + 1; i++)
                    {
                        outStream.Add(compressed[inIndex]);
                        inIndex = inIndex + 1;
                        outIndex = outIndex + 1;
                    }
                }
                else
                {
                    var length = ctrl >> 5;
                    if (length == 7)
                    {
                        length = length + compressed[inIndex];
                        inIndex = inIndex + 1;
                    }

                    var @ref = outIndex - ((ctrl & 0x1f) << 8) - compressed[inIndex] - 1;
                    inIndex = inIndex + 1;

                    for (int i = 0; i < length + 2; i++)
                    {
                        outStream.Add(outStream[@ref]);
                        @ref = @ref + 1;
                        outIndex = outIndex + 1;
                    }
                }
            }

            return outStream.ToArray();
        }

        public static System.UInt32 ReadUInt32BigEndian(this BinaryReader br)
        {
            var bytes = br.ReadBytes(4);
            System.Array.Reverse(bytes);
            return System.BitConverter.ToUInt32(bytes, 0);
        }

        public static System.UInt64 ReadInt64BigEndian(this BinaryReader br)
        {
            var bytes = br.ReadBytes(8);
            System.Array.Reverse(bytes);
            return System.BitConverter.ToUInt64(bytes, 0);
        }

        public static float ReadFloat(this BinaryReader br)
        {
            var len = br.ReadByte();

            if (len == 253) return float.NaN;
            else if (len == 254) return float.PositiveInfinity;
            else if (len == 255) return float.NegativeInfinity;

            var data = br.ReadBytes(len);
            var str = System.Text.Encoding.UTF8.GetString(data, 0, len);
            return float.TryParse(str, out var res)
                ? res
                : 0;
        }

        public static void SkipStr(this BinaryReader br)
        {
            ulong bytesToSkip = 0;

            var (length, isEncoded) = ReadLengthWithEncoding(br);

            if (!isEncoded)
            {
                bytesToSkip = length;
            }
            else
            {
                if (length == Constant.EncType.INT8)
                {
                    bytesToSkip = 1;
                }
                else if (length == Constant.EncType.INT16)
                {
                    bytesToSkip = 2;
                }
                else if (length == Constant.EncType.INT32)
                {
                    bytesToSkip = 4;
                }
                else if (length == Constant.EncType.LZF)
                {
                    var clen = ReadLength(br);
                    _ = ReadLength(br);

                    bytesToSkip = clen;
                }
            }

            if (bytesToSkip > 0) br.ReadBytes((int)bytesToSkip);
        }

        public static void SkipModule(this BinaryReader br)
        {
            _ = ReadLength(br);
            var opCode = ReadLength(br);

            while (opCode != Constant.ModuleOpCode.EOF)
            {
                if (opCode == Constant.ModuleOpCode.SINT
                    || opCode == Constant.ModuleOpCode.UINT)
                {
                    _ = ReadLength(br);
                }
                else if (opCode == Constant.ModuleOpCode.FLOAT)
                {
                    _ = br.ReadBytes(4);
                }
                else if (opCode == Constant.ModuleOpCode.DOUBLE)
                {
                    _ = br.ReadBytes(8);
                }
                else if (opCode == Constant.ModuleOpCode.STRING)
                {
                    SkipStr(br);
                }
                else
                {
                    throw new RDBParserException($"Unknown module opcode {opCode}");
                }

                opCode = ReadLength(br);
            }
        }

        public static void SkipStream(this BinaryReader br)
        {
            var listPacks = ReadLength(br);

            while (listPacks > 0)
            {
                _ = ReadStr(br);
                _ = ReadStr(br);

                listPacks--;
            }

            _ = ReadLength(br);
            _ = ReadLength(br);
            _ = ReadLength(br);

            var cgroups = ReadLength(br);
            while (cgroups > 0)
            {
                _ = ReadStr(br);
                _ = ReadLength(br);
                _ = ReadLength(br);
                var pending = ReadLength(br);
                while (pending > 0)
                {
                    _ = br.ReadBytes(16);
                    _ = br.ReadBytes(8);
                    _ = ReadLength(br);

                    pending--;
                }
                var consumers = ReadLength(br);
                while (consumers > 0)
                {
                    SkipStr(br);
                    br.ReadBytes(8);
                    pending = ReadLength(br);
                    br.ReadBytes((int)(pending * 16));

                    consumers--;
                }
            }
        }
    }
}

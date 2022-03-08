using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser
    {
        public (ulong Length, bool IsEncoded) ReadLength(BinaryReader br)
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

        public byte[] ReadString(BinaryReader br)
        {
            var (length, isEncoded) = ReadLength(br);

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
                var clen = ReadLength(br).Length;
                var ulen = ReadLength(br).Length;

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

        public void ReadObject(BinaryReader br, byte[] key, int encType, long expiry, Info info)
        {
            if (encType == Constant.DataType.STRING)
            {
                var value = ReadString(br);

                info.Encoding = "string";
                _callback.Set(key, value, expiry, info);
            }
            else if (encType == Constant.DataType.LIST)
            {
                var length = ReadLength(br).Length;
                info.Encoding = "linkedlist";
                _callback.StartList(key, expiry, info);
                while (length > 0)
                {
                    length--;
                    var val = ReadString(br);
                    _callback.RPush(key, val);
                }
                _callback.EndList(key, info);
            }
            else if (encType == Constant.DataType.SET)
            {
                var cardinality = ReadLength(br).Length;
                info.Encoding = "hashtable";
                _callback.StartSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = ReadString(br);
                    _callback.SAdd(key, member);
                }
                _callback.EndSet(key);
            }
            else if (encType == Constant.DataType.ZSET || encType == Constant.DataType.ZSET_2)
            {
                var cardinality = ReadLength(br).Length;
                info.Encoding = "skiplist";
                _callback.StartSortedSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = ReadString(br);

                    double score = encType == Constant.DataType.ZSET_2
                        ? br.ReadDouble()
                        : ReadFloat(br);

                    _callback.ZAdd(key, score, member);
                }
                _callback.EndSortedSet(key);
            }
            else if (encType == Constant.DataType.HASH)
            {
                var length = ReadLength(br).Length;

                info.Encoding = "hashtable";
                _callback.StartHash(key, (long)length, expiry, info);

                while (length > 0)
                {
                    length--;
                    var field = ReadString(br);
                    var value = ReadString(br);

                    _callback.HSet(key, field, value);
                }
                _callback.EndHash(key);
            }
            else if (encType == Constant.DataType.HASH_ZIPMAP)
            {
                ReadZipMap(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.LIST_ZIPLIST)
            {
                ReadZipList(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.SET_INTSET)
            {
                ReadIntSet(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.ZSET_ZIPLIST)
            {
                ReadZSetFromZiplist(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.HASH_ZIPLIST)
            {
                ReadHashFromZiplist(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.LIST_QUICKLIST)
            {
                ReadListFromQuickList(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.MODULE)
            {
                throw new RDBParserException($"Unable to read Redis Modules RDB objects (key {key})");
            }
            else if (encType == Constant.DataType.MODULE_2) 
            {
                SkipModule(br);
            }
            else if (encType == Constant.DataType.STREAM_LISTPACKS) 
            {
                SkipStream(br);
            }
            else
            {
                throw new RDBParserException($"Invalid object type {encType} for {key} ");
            }
        }

        private void SkipStream(BinaryReader br)
        {
            var listPacks = ReadLength(br).Length;

            while (listPacks > 0)
            {
                _ = ReadString(br);
                _ = ReadString(br);

                listPacks--;
            }

            _ = ReadLength(br);
            _ = ReadLength(br);
            _ = ReadLength(br);

            var cgroups = ReadLength(br).Length;
            while (cgroups > 0)
            {
                _ = ReadString(br);
                _ = ReadLength(br);
                _ = ReadLength(br);
                var pending = ReadLength(br).Length;
                while (pending > 0)
                {
                    _ = br.ReadBytes(16);
                    _ = br.ReadBytes(8);
                    _ = ReadLength(br);

                    pending--;
                }
                var consumers = ReadLength(br).Length;
                while (consumers > 0)
                {
                    SkipString(br);
                    br.ReadBytes(8);
                    pending = ReadLength(br).Length;
                    br.ReadBytes((int)(pending * 16));

                    consumers--;
                }
            }
        }

        private void SkipModule(BinaryReader br)
        {
            _ = ReadLength(br);
            var opCode = ReadLength(br).Length;

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
                    SkipString(br);
                }
                else
                {
                    throw new RDBParserException($"Unknown module opcode {opCode}");
                }

                opCode = ReadLength(br).Length;
            }
        }

        private float ReadFloat(BinaryReader br)
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

        private void SkipString(BinaryReader br)
        {
            ulong bytesToSkip = 0;

            var (length, isEncoded) = ReadLength(br);

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
                    var clen = ReadLength(br).Length;
                    _ = ReadLength(br).Length;

                    bytesToSkip = clen;
                }
            }

            if (bytesToSkip > 0) br.ReadBytes(bytesToSkip);
        }
    }

}

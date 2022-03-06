using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    public class DefaultRDBParser : IRDBParser
    {
        private readonly IReaderCallback _callback;

        public DefaultRDBParser(IReaderCallback callback)
        {
            this._callback = callback;
        }

        public void Parse(string path)
        {
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                using (BinaryReader br = new BinaryReader(fs))
                {
                    var magicStringBytes = br.ReadBytes(Constant.MagicCount.REDIS);
                    BasicVerify.CheckRedisMagicString(magicStringBytes);

                    var versionBytes = br.ReadBytes(Constant.MagicCount.VERSION);
                    var version = BasicVerify.CheckAndGetRDBVersion(versionBytes);
                    _callback.StartRDB(version);

                    ulong db = 0;
                    long expiry = 0;
                    bool isFirstDb = true;

                    while (true)
                    {
                        ulong lruIdle = 0;
                        int lfuFreq = 0;

                        var opType = br.ReadByte();

                        if (opType == Constant.OpCode.EXPIRETIME_MS)
                        {
                            expiry = br.ReadInt64();
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.EXPIRETIME)
                        {
                            expiry = br.ReadInt32();
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.IDLE)
                        {
                            var idle = ReadLength(br).Length;
                            lruIdle = idle;
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.FREQ)
                        {
                            var freq = br.ReadByte();
                            lfuFreq = freq;
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.SELECTDB)
                        {
                            if (!isFirstDb)
                                _callback.EndDatabase((int)db);

                            db = ReadLength(br).Length;
                            _callback.StartDatabase((int)db);
                            continue;
                        }

                        if (opType == Constant.OpCode.AUX)
                        {
                            var auxKey = ReadString(br);
                            var auxVal = ReadString(br);
                            _callback.AuxField(auxKey, auxVal);
                            continue;
                        }

                        if (opType == Constant.OpCode.RESIZEDB)
                        {
                            var dbSize = ReadLength(br).Length;
                            var expireSize = ReadLength(br).Length;

                            _callback.DbSize((uint)dbSize, (uint)expireSize);
                            continue;
                        }

                        if (opType == Constant.OpCode.MODULE_AUX)
                        {
                            // TODO
                            System.Console.WriteLine("MODULE_AUX");
                        }

                        if (opType == Constant.OpCode.EOF)
                        {
                            _callback.EndDatabase((int)db);
                            _callback.EndRDB();

                            if (version >= 5) br.ReadBytes(Constant.MagicCount.CHECKSUM);

                            break;
                        }

                        var key = ReadString(br);

                        Info info = new Info
                        {
                            Idle = lruIdle,
                            Freq = lfuFreq
                        };

                        ReadObject(br, key, opType, expiry, info);

                        expiry = 0;
                    }

                }
            }
        }

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
            else if (encType == Constant.LengthEncoding.BIT32)
            {
                len = br.ReadUInt32();
            }
            else if (encType == Constant.LengthEncoding.BIT64)
            {
                len = br.ReadUInt64();
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
            else if (encType == Constant.DataType.MODULE_2) { }
            else if (encType == Constant.DataType.STREAM_LISTPACKS) { }
            else
            {
                throw new RDBParserException($"Invalid object type {encType} for {key} ");
            }
        }

        private void ReadHashFromZiplist(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var raw = ReadString(br);
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
            var raw = ReadString(br);
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

        private void ReadIntSet(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var raw = ReadString(br);
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var encoding = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            info.Encoding = "intset";
            info.SizeOfValue = raw.Length;
            _callback.StartList(key, expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                if (encoding != 8 & encoding == 4 & encoding == 2)
                    throw new RDBParserException($"Invalid encoding {encoding} for key {key}");

                var entry = rd.ReadBytes((int)encoding);
                _callback.SAdd(key, entry);
            }

            _callback.EndSet(key);
        }

        private void ReadZipList(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var ziplist = ReadString(br);
            using MemoryStream stream = new MemoryStream(ziplist);
            using var rd = new BinaryReader(stream);
            var zlbytes = rd.ReadUInt32();
            var tail_offset = rd.ReadUInt32();
            var numEntries = rd.ReadUInt16();

            info.Encoding = "ziplist";
            info.SizeOfValue = ziplist.Length;
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

        private void ReadZipMap(BinaryReader br, byte[] key, long expiry, Info info)
        {
            int length = 0;
            var zipMap = ReadString(br);
            using MemoryStream stream = new MemoryStream(zipMap);
            using var rd = new BinaryReader(stream);
            var lenByte = rd.ReadByte();

            info.Encoding = "zipmap";
            info.SizeOfValue = zipMap.Length;
            _callback.StartHash(key, length, expiry, info);

            while (true)
            {
                var nextLength = ReadZipmapNextLength(rd);
                if (!nextLength.HasValue) break;

                var filed = rd.ReadBytes((int)nextLength);

                nextLength = ReadZipmapNextLength(rd);
                if (!nextLength.HasValue) throw new RDBParserException($"Unexepcted end of zip map for key {key}");

                var free = rd.ReadByte();
                var value = rd.ReadBytes((int)nextLength);

                if (free > 0) rd.ReadBytes((int)free);

                _callback.HSet(key, filed, value);
            }

            _callback.EndHash(key);
        }

        private int? ReadZipmapNextLength(BinaryReader br)
        {
            var num = br.ReadByte();
            if (num < 254) return num;
            else if (num == 254) return (int?)br.ReadUInt32();
            else return num;
        }

        private byte[] LzfDecompress(byte[] compressed, int ulen)
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

        private void ReadListFromQuickList(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var length = ReadLength(br).Length;
            var totalSize = 0;
            info.Encoding = "quicklist";
            info.Zips = length;
            _callback.StartList(key, expiry, info);

            while (length > 0)
            {
                length--;

                var rawString = ReadString(br);
                totalSize += rawString.Length;

                using (MemoryStream stream = new MemoryStream(rawString))
                {
                    var rd = new BinaryReader(stream);
                    var zlbytes = rd.ReadBytes(4);
                    var tail_offset = rd.ReadBytes(4);
                    var num_entries = rd.ReadUInt16();

                    for (int i = 0; i < num_entries; i++)
                    {
                        _callback.RPush(key, ReadZipListEntry(rd));
                    }

                    var zlistEnd = rd.ReadByte();
                    if (zlistEnd != 255)
                    {
                        throw new RDBParserException("Invalid zip list end");
                    }
                }
            }

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
                length = (entryHeader & 0x3F) >> 8 | br.ReadByte();
                value = br.ReadBytes(length);
            }
            else if (entryHeader >> 6 == 2)
            {
                length = (int)br.ReadUInt32();
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
    }
}

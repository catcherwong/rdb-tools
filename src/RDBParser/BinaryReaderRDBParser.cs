using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private readonly IReaderCallback _callback;

        public BinaryReaderRDBParser(IReaderCallback callback)
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
                            ReadModule(br);
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

        private void ReadZipMap(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var rawString = ReadString(br);
            using MemoryStream stream = new MemoryStream(rawString);
            using var rd = new BinaryReader(stream);
            var numEntries = rd.ReadByte();

            info.Encoding = "zipmap";
            info.SizeOfValue = rawString.Length;
            _callback.StartHash(key, numEntries, expiry, info);

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
            else return null;
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

        private void ReadModule(BinaryReader br)
        {
            //var length = ReadLength(br).Length;
            //_callback.StartModule(null, null, 0, null);
        }
    }
}

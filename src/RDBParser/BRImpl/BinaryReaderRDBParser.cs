using System.IO;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private readonly IBinaryReaderCallback _callback;
        private byte[] _key = null;
        private long _expiry = 0;
        private ulong _idle = 0;
        private int _freq = 0;

        public BinaryReaderRDBParser(IBinaryReaderCallback callback)
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
                    BinaryReaderBasicVerify.CheckRedisMagicString(magicStringBytes);

                    var versionBytes = br.ReadBytes(Constant.MagicCount.VERSION);
                    var version = BinaryReaderBasicVerify.CheckAndGetRDBVersion(versionBytes);
                    _callback.StartRDB(version);

                    ulong db = 0;
                    bool isFirstDb = true;

                    while (true)
                    {
                        Info info = new Info();

                        var opType = br.ReadByte();
                        if (opType == Constant.OpCode.EXPIRETIME_MS)
                        {
                            _expiry = br.ReadInt64();
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.EXPIRETIME)
                        {
                            _expiry = br.ReadInt32();
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.IDLE)
                        {
                            _idle = br.ReadLength();
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.FREQ)
                        {
                            _freq = br.ReadByte();
                            opType = br.ReadByte();
                        }

                        if (opType == Constant.OpCode.SELECTDB)
                        {
                            if (!isFirstDb)
                                _callback.EndDatabase((int)db);

                            isFirstDb = false;
                            db = br.ReadLength();
                            _callback.StartDatabase((int)db);
                            continue;
                        }

                        if (opType == Constant.OpCode.AUX)
                        {
                            var auxKey = br.ReadStr();
                            var auxVal = br.ReadStr();
                            _callback.AuxField(auxKey, auxVal);
                            continue;
                        }

                        if (opType == Constant.OpCode.RESIZEDB)
                        {
                            var dbSize = br.ReadLength();
                            var expireSize = br.ReadLength();

                            _callback.DbSize((uint)dbSize, (uint)expireSize);
                            continue;
                        }

                        if (opType == Constant.OpCode.MODULE_AUX)
                        {
                            info.Idle = _idle;
                            info.Freq = _freq;

                            ReadModule(br, _key, opType, _expiry, info);
                            continue;
                        }

                        if (opType == Constant.OpCode.EOF)
                        {
                            _callback.EndDatabase((int)db);
                            _callback.EndRDB();

                            if (version >= 5) br.ReadBytes(Constant.MagicCount.CHECKSUM);

                            break;
                        }

                        _key = br.ReadStr();

                        info.Idle = _idle;
                        info.Freq = _freq;

                        ReadObject(br, _key, opType, _expiry, info);

                        _expiry = 0;
                    }
                }
            }
        }

        public Task ParseAsync(string path)
            => Task.Run(() => Parse(path));

        private void ReadIntSet(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var raw = br.ReadStr();
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

        private void ReadListFromQuickList(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var length = br.ReadLength();
            var totalSize = 0;
            info.Encoding = "quicklist";
            info.Zips = length;
            _callback.StartList(key, expiry, info);

            while (length > 0)
            {
                length--;

                var rawString = br.ReadStr();
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
    }
}

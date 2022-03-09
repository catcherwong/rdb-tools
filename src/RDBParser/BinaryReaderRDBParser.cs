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
                            var idle = br.ReadLength(); ;
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

                            db = br.ReadLength(); ;
                            _callback.StartDatabase((int)db);
                            continue;
                        }

                        if (opType == Constant.OpCode.AUX)
                        {
                            var auxKey = br.ReadStr(); ;
                            var auxVal = br.ReadStr(); ;
                            _callback.AuxField(auxKey, auxVal);
                            continue;
                        }

                        if (opType == Constant.OpCode.RESIZEDB)
                        {
                            var dbSize = br.ReadLength(); ;
                            var expireSize = br.ReadLength(); ;

                            _callback.DbSize((uint)dbSize, (uint)expireSize);
                            continue;
                        }

                        if (opType == Constant.OpCode.MODULE_AUX)
                        {
                            ReadModule(br, null, opType, expiry, null);
                        }

                        if (opType == Constant.OpCode.EOF)
                        {
                            _callback.EndDatabase((int)db);
                            _callback.EndRDB();

                            if (version >= 5) br.ReadBytes(Constant.MagicCount.CHECKSUM);

                            break;
                        }

                        var key = br.ReadStr();

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
            var raw = br.ReadStr(); ;
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
            var rawString = br.ReadStr(); ;
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

        private void ReadListFromQuickList(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var length = br.ReadLength(); ;
            var totalSize = 0;
            info.Encoding = "quicklist";
            info.Zips = length;
            _callback.StartList(key, expiry, info);

            while (length > 0)
            {
                length--;

                var rawString = br.ReadStr(); ;
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

        private void ReadModule(BinaryReader br, byte[] key, int encType, long expiry, Info info)
        {
            var wrapper = new IOWrapper(br.BaseStream);
            wrapper.StartRecordingSize();
            wrapper.StartRecording();
            var length = wrapper.ReadLength();
            var isRecordBuffer = _callback.StartModule(key, DecodeModuleId(length), expiry, info);

            if (!isRecordBuffer) wrapper.StopRecording();

            var opCode = wrapper.ReadLength();

            while (opCode != Constant.ModuleOpCode.EOF)
            {
                byte[] data;

                if (opCode == Constant.ModuleOpCode.SINT
                    || opCode == Constant.ModuleOpCode.UINT)
                {
                    data = new byte[] { (byte)wrapper.ReadLength() };
                }
                else if (opCode == Constant.ModuleOpCode.FLOAT)
                {
                    data = wrapper.ReadBytes(4);
                }
                else if (opCode == Constant.ModuleOpCode.DOUBLE)
                {
                    data = wrapper.ReadBytes(8);
                }
                else if (opCode == Constant.ModuleOpCode.STRING)
                {
                    data = wrapper.ReadStr();
                }
                else
                {
                    throw new RDBParserException($"Unknown module opcode {opCode}");
                }

                _callback.HandleModuleData(null, opCode, data);

                opCode = wrapper.ReadLength();
            }

            byte[] buff = null;

            if (isRecordBuffer)
            {
                var tmp = new List<byte>();
                tmp.Add(0x07);
                tmp.AddRange(wrapper.GetRecordedBuff());
                buff = tmp.ToArray();
                wrapper.StopRecording();
            }

            _callback.EndModule(null, wrapper.GetRecordedSize(), buff);
        }


        private static string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        private string DecodeModuleId(ulong moduleId)
        {
            int len = 9;

            var name = new string[len];
            moduleId >>= 10;

            while (len > 0)
            {
                len--;
                var idx = moduleId & 63;
                name[len] = charset[(int)idx].ToString();
                moduleId >>= 6;
            }

            return string.Join("", name);
        }
        
        public class IOWrapper : BinaryReader
        {
            private bool _recordBuff;
            private bool _recordBuffSize;
            private List<byte> _bytes;
            private long _buffSize;

            public IOWrapper(Stream input) : base(input)
            {
            }

            public void StartRecording()
                => _recordBuff = true;

            public void StartRecordingSize()
                => _recordBuffSize = true;

            public byte[] GetRecordedBuff()
                => _bytes.ToArray();

            public long GetRecordedSize()
                => _buffSize;

            public void StopRecording()
            {
                _recordBuff = false;
                _bytes = new List<byte>();
            }

            public void StopRecordingSize()
            {
                _recordBuffSize = false;
                _buffSize = 0;
            }

            public override byte ReadByte()
            {
                var b = base.ReadByte();

                if (_recordBuff) _bytes.Add(b);

                if (_recordBuffSize) _buffSize += 1;

                return b;

            }

            public override byte[] ReadBytes(int count)
            {
                var bytes = base.ReadBytes(count);

                if (_recordBuff) _bytes.AddRange(bytes);

                if (_recordBuffSize) _buffSize += bytes.Length;

                return bytes;
            }
        }
    }
}

using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private readonly IReaderCallback _callback;
        private ParserFilter _filter;
        private byte[] _key = null;
        private long _expiry = 0;
        private ulong _idle = 0;
        private int _freq = 0;

        public BinaryReaderRDBParser(IReaderCallback callback, ParserFilter filter = null)
        {
            this._callback = callback;
            this._filter = filter;
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

                            ReadModule(br);
                            continue;
                        }

                        if (opType == Constant.OpCode.FUNCTION || opType == Constant.OpCode.FUNCTION2)
                        { 
                            // TODO function
                        }

                        if (opType == Constant.OpCode.EOF)
                        {
                            _callback.EndDatabase((int)db);
                            _callback.EndRDB();

                            if (version >= 5) br.ReadBytes(Constant.MagicCount.CHECKSUM);

                            break;
                        }

                        if (MatchFilter(database: (int)db))
                        {
                            _key = br.ReadStr();

                            if (MatchFilter(dataType: opType))
                            {
                                info.Idle = _idle;
                                info.Freq = _freq;

                                ReadObject(br, _key, opType, _expiry, info);
                            }
                            else
                            { 
                                SkipObject(br, opType);
                            }
                            
                            _expiry = 0;
                        }
                        else
                        {
                            br.SkipStr();
                            SkipObject(br, opType);
                        }
                    }
                }
            }
        }

        public Task ParseAsync(string path)
            => Task.Run(() => Parse(path));

        private bool MatchFilter(int database = -1, int dataType = -1)
        {
            if (_filter == null) return true;

            // filter databse
            if (database >= 0 
                && _filter.Databases != null 
                && _filter.Databases.Any()
                && !_filter.Databases.Contains(database))
            {
                return false;
            }

            // filter data type
            if (dataType >= 0
                && _filter.Types != null
                && _filter.Types.Any()
                && !_filter.Types.Contains(GetLogicalType(dataType)))
            {
                return false;
            }

            return true;
        }

        private string GetLogicalType(int type)
            => Constant.DataType.MAPPING[type];
    }
}

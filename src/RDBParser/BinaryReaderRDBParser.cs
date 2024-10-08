﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        private int _mem_policy = -1; // 1 - lru | 2 - lfu
        private int _version = -1;
        private ulong _slotId = 0;
        private HashSet<string> _auxKey = new HashSet<string>();

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
                    _version = version;
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
                            // What the value means? It means that the key was not accessed in `_idle` seconds
                            // For example(volatile-lru), `set e1 v1 ex 6000`, `set e2 v2 ex 600`
                            // get e1 at 01:00:00, get e2 at 01:00:05, bgsave at 01:00:10
                            // e1's idle = 10s, e2's idle = 5s
                            _idle = br.ReadLength();
                            opType = br.ReadByte();

                            if (_mem_policy == -1)
                            {
                                _mem_policy = 1;
                                _callback.SetIdleOrFreq(1);
                            }
                        }

                        if (opType == Constant.OpCode.FREQ)
                        {
                            // 0 ~ 255
                            _freq = br.ReadByte();
                            opType = br.ReadByte();

                            if (_mem_policy == -1)
                            {
                                _mem_policy = 2;
                                _callback.SetIdleOrFreq(2);
                            }
                        }

                        if (opType == Constant.OpCode.SLOTINFO)
                        {
                            // cluster keyslot yourkey
                            _slotId = br.ReadLength();

                            // slotSize
                            _ = br.ReadLength();

                            // expireSlotSize
                            _ = br.ReadLength();
                            continue;
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

                            var k = Encoding.UTF8.GetString(auxKey);
                            _auxKey.Add(k);

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

                        if (opType == Constant.OpCode.FUNCTION)
                        {
                            // https://github.com/redis/redis/blob/7.0-rc3/src/rdb.c#L2804-L2841
                            var name = br.ReadStr();
                            var engineName = br.ReadStr();
                            var hasDesc = br.ReadLength();
                            if (hasDesc>0)
                            {
                                var desc = br.ReadStr();
                            }

                            var blob = br.ReadStr();

                            _callback.FuntionLoad(engineName, name, blob);
                            continue;
                        }

                        if (opType == Constant.OpCode.FUNCTION2)
                        {
                            // https://github.com/redis/redis/blob/7.0-rc3/src/rdb.c#L2849
                            var finalPayload = br.ReadStr();
                            var (engine, libName, code) = RedisRdbObjectHelper.ExtractLibMetaData(finalPayload);

                            _callback.FuntionLoad(engine, libName, code);
                            continue;
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
                            
                            if (MatchFilter(dataType: opType, key: _key))
                            {
                                info.Idle = _idle;
                                info.Freq = _freq;
                                info.SlotId = _slotId;

                                ReadObject(br, _key, opType, _expiry, info);
                            }
                            else
                            { 
                                SkipObject(br, opType);
                            }
                        }
                        else
                        {
                            br.SkipStr();
                            SkipObject(br, opType);
                        }

                        _expiry = 0;
                    }
                }
            }
        }

        public Task ParseAsync(string path)
            => Task.Run(() => Parse(path));

        public bool IsValkey()
            => _auxKey.Contains("valkey-ver");

        private bool MatchFilter(int database = -1, int dataType = -1, byte[] key = null)
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

            if(key != null
               && _filter.KeyPrefixes != null
               && _filter.KeyPrefixes.Any())
            {
                var keyStr = System.Text.Encoding.UTF8.GetString(key);
                
                bool flag = false;

                foreach(var item in _filter.KeyPrefixes)
                {
                    flag = flag || keyStr.StartsWith(item, System.StringComparison.OrdinalIgnoreCase);
                }

                return flag;
            }

            if(_filter.IsPermanent.HasValue)
            {
                if (_filter.IsPermanent.Value)
                {
                    return _expiry == 0;
                }
                else
                {
                    return _expiry != 0;
                }
            }

            if (_filter.IsExpired.HasValue)
            {
                if (_expiry == 0 || _expiry >= Constant.MaxExpireTimestamp)
                { 
                    return false;
                }

                var sub = DateTimeOffset.FromUnixTimeMilliseconds(_expiry).Subtract(DateTimeOffset.UtcNow);
                if (_filter.IsExpired.Value)
                {
                    return sub.TotalHours <= 0;
                }
                else
                { 
                    return sub.TotalHours > 0;
                }
            }

            if (_filter.MinIdle.HasValue)
            {
                return _idle > _filter.MinIdle.Value;
            }


            if (_filter.MinFreq.HasValue)
            {
                return _freq > _filter.MinFreq.Value;
            }

            return true;
        }

        private string GetLogicalType(int type)
            => Constant.DataType.MAPPING[type];
    }
}

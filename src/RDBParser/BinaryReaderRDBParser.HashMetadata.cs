using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private const ulong EB_EXPIRE_TIME_MAX = 0x0000FFFFFFFFFFFF;
        private const ulong EB_EXPIRE_TIME_INVALID = EB_EXPIRE_TIME_MAX + 1;

        private void ReadHashMetadata(BinaryReader br, int encType)
        {
            // https://github.com/redis/redis/blob/c9d29f6a918c335bc1778d9f68e521c1bbb36a0f/src/rdb.c#L2265
            ulong minExpire = EB_EXPIRE_TIME_INVALID;
            if (encType == Constant.DataType.HASH_METADATA)
            {
                minExpire = br.ReadUInt64();
            }

            var len = br.ReadLength();

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = "";
            info.SizeOfValue = (int)len;
            _callback.StartHash(_key, (int)len, _expiry, info);
            
            ulong expireAt = 0;
            while (len > 0)
            {
                var ttl = br.ReadLength();

                if (encType == Constant.DataType.HASH_METADATA)
                {
                    expireAt = ttl != 0 ? ttl + minExpire - 1 : 0;
                }
                else
                {
                    expireAt = ttl;
                }

                var field = br.ReadStr();
                var value = br.ReadStr();

                _callback.HSet(_key, field, value, (long)expireAt);

                len--;
            }

            _callback.EndHash(_key);
        }

        private void SkipHashMetadata(BinaryReader br, int encType)
        {
            // https://github.com/redis/redis/blob/c9d29f6a918c335bc1778d9f68e521c1bbb36a0f/src/rdb.c#L2265           
            if (encType == Constant.DataType.HASH_METADATA)
            {
                _ = br.ReadUInt64();
            }

            var len = br.ReadLength();
            while (len > 0)
            {
                _ = br.ReadLength();
                _ = br.ReadStr();
                _ = br.ReadStr();

                len--;
            }
        }
    }
}

using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadIntSet(BinaryReader br)
        {
            // https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#intset-encoding
            var raw = br.ReadStr();
            using MemoryStream stream = new MemoryStream(raw);
            using var rd = new BinaryReader(stream);
            var encoding = rd.ReadUInt32();

            if(encoding != 8 & encoding == 4 & encoding == 2)
                throw new RDBParserException($"Invalid encoding {encoding} for key {_key}");

            var numEntries = rd.ReadUInt32();

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = Constant.ObjEncoding.INTSET;
            info.SizeOfValue = raw.Length;
            _callback.StartSet(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                string entry = string.Empty;
                var tmp = rd.ReadBytes((int)encoding);

                if (encoding == 8)
                {
                    entry = System.BitConverter.ToInt64(tmp).ToString();
                }
                else if (encoding == 4)
                {
                    entry = System.BitConverter.ToInt32(tmp).ToString();
                }
                else if (encoding == 2)
                {
                    entry = System.BitConverter.ToInt16(tmp).ToString();
                }

                _callback.SAdd(_key, System.Text.Encoding.UTF8.GetBytes(entry));
            }

            _callback.EndSet(_key);
        }
    }
}

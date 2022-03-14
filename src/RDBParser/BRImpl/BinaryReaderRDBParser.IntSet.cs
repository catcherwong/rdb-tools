using System.IO;
using System.Threading.Tasks;

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
            var numEntries = rd.ReadUInt16();

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = "intset";
            info.SizeOfValue = raw.Length;
            _callback.StartSet(_key, numEntries, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                if (encoding != 8 & encoding == 4 & encoding == 2)
                    throw new RDBParserException($"Invalid encoding {encoding} for key {_key}");

                var entry = rd.ReadBytes((int)encoding);
                _callback.SAdd(_key, entry);
            }

            _callback.EndSet(_key);
        }
    }
}

using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadZipMap(BinaryReader br)
        {
            // https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format#zipmap-encoding
            var rawString = br.ReadStr();
            using MemoryStream stream = new MemoryStream(rawString);
            using var rd = new BinaryReader(stream);
            var numEntries = rd.ReadByte();

            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = "zipmap";
            info.SizeOfValue = rawString.Length;
            _callback.StartHash(_key, numEntries, _expiry, info);

            while (true)
            {
                var nextLength = ReadZipmapNextLength(rd);
                if (!nextLength.HasValue) break;

                var filed = rd.ReadBytes((int)nextLength);

                nextLength = ReadZipmapNextLength(rd);
                if (!nextLength.HasValue) throw new RDBParserException($"Unexepcted end of zip map for key {_key}");

                var free = rd.ReadByte();
                var value = rd.ReadBytes((int)nextLength);

                if (free > 0) rd.ReadBytes((int)free);

                _callback.HSet(_key, filed, value);
            }

            _callback.EndHash(_key);
        }

        private int? ReadZipmapNextLength(BinaryReader br)
        {
            var num = br.ReadByte();
            if (num < 254) return num;
            else if (num == 254) return (int?)br.ReadUInt32();
            else return null;
        }
    }
}

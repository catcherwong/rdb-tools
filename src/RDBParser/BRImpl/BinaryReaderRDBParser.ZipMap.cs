
using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadZipMap(BinaryReader br, byte[] key, long expiry, Info info)
        {
            var rawString = br.ReadStr();
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
    }
}

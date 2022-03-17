using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {
        private async Task ReadZipMapAsync(PipeReader reader, Info info)
        {
            var rawString = await reader.ReadStringAsync();

            var rd = PipeReader.Create(rawString);
            var numEntries = await rd.ReadSingleByteAsync();

            info.Encoding = "zipmap";
            info.SizeOfValue = (int)rawString.Length;
            _callback.StartHash(_key, numEntries, _expiry, info);

            while (true)
            {
                var nextLength = await ReadZipmapNextLengthAsync(rd);
                if (!nextLength.HasValue) break;

                var filed = await rd.ReadBytesAsync((int)nextLength);

                nextLength = await ReadZipmapNextLengthAsync(rd);
                if (!nextLength.HasValue) throw new RDBParserException($"Unexepcted end of zip map for key {_key}");

                var free = await rd.ReadSingleByteAsync();
                var value = await rd.ReadBytesAsync((int)nextLength);

                if (free > 0) await rd.ReadBytesAsync((int)free);

                _callback.HSet(_key, filed.ToArray(), value.ToArray());
            }

            _callback.EndHash(_key);
        }

        private async Task<int?> ReadZipmapNextLengthAsync(PipeReader reader)
        {
            var num = await reader.ReadSingleByteAsync();
            if (num < 254) return num;
            else if (num == 254)
            { 
                var buff = await reader.ReadBytesAsync(4);
                return (int?)buff.ReadUInt32LittleEndianItem();
            }

            else return null;
        }
    }
}

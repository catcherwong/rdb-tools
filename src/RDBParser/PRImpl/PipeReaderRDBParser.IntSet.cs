using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    { 
        private async Task ReadIntSetAsync(PipeReader reader, Info info)
        {
            var raw = await reader.ReadStringAsync();
            var rd = PipeReader.Create(raw);

            var encodingBuff = await rd.ReadBytesAsync(4);
            var encoding = encodingBuff.ReadUInt32LittleEndianItem();

            var numEntriesBuff = await rd.ReadBytesAsync(2);
            var numEntries = numEntriesBuff.ReadUInt16LittleEndianItem();

            info.Encoding = "intset";
            info.SizeOfValue = (int)raw.Length;
            _callback.StartList(_key, _expiry, info);

            for (int i = 0; i < numEntries; i++)
            {
                if (encoding != 8 & encoding == 4 & encoding == 2)
                    throw new RDBParserException($"Invalid encoding {encoding} for key {_key}");

                var entry = await rd.ReadBytesAsync((int)encoding);
                _callback.SAdd(_key, entry);
            }

            _callback.EndSet(_key);
        }
    }
}

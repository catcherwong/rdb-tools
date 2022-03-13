using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {
        private async Task SkipStreamAsync(PipeReader reader)
        {
            var listPacks = await reader.ReadLengthAsync();

            while (listPacks > 0)
            {
                _ = await reader.ReadStringAsync();
                _ = await reader.ReadStringAsync();

                listPacks--;
            }

            _ = await reader.ReadLengthAsync();
            _ = await reader.ReadLengthAsync();
            _ = await reader.ReadLengthAsync();

            var cgroups = await reader.ReadLengthAsync();
            while (cgroups > 0)
            {
                _ = await reader.ReadStringAsync();
                _ = await reader.ReadLengthAsync();
                _ = await reader.ReadLengthAsync();
                var pending = await reader.ReadLengthAsync();
                while (pending > 0)
                {
                    _ = await reader.ReadBytesAsync(16);
                    _ = await reader.ReadBytesAsync(8);
                    _ = await reader.ReadLengthAsync();

                    pending--;
                }
                var consumers = await reader.ReadLengthAsync();
                while (consumers > 0)
                {
                    await reader.SkipStringAsync();
                    await reader.ReadBytesAsync(8);
                    pending = await reader.ReadLengthAsync();
                    await reader.ReadBytesAsync((int)(pending * 16));

                    consumers--;
                }

                cgroups--;
            }
        }

    }
}

using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser
    {
        private async Task SkipModuleAsync(PipeReader reader)
        {
            _ = await reader.ReadLengthAsync();
            var opCode = await reader.ReadLengthAsync();

            while (opCode != Constant.ModuleOpCode.EOF)
            {
                if (opCode == Constant.ModuleOpCode.SINT
                    || opCode == Constant.ModuleOpCode.UINT)
                {
                    _ = await reader.ReadLengthAsync();
                }
                else if (opCode == Constant.ModuleOpCode.FLOAT)
                {
                    _ = await reader.ReadBytesAsync(4);
                }
                else if (opCode == Constant.ModuleOpCode.DOUBLE)
                {
                    _ = await reader.ReadBytesAsync(8);
                }
                else if (opCode == Constant.ModuleOpCode.STRING)
                {
                    await reader.SkipStringAsync();
                }
                else
                {
                    throw new RDBParserException($"Unknown module opcode {opCode}");
                }

                opCode = await reader.ReadLengthAsync();
            }
        }
    }
}

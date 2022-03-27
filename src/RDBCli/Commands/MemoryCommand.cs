using System.CommandLine;
using System.CommandLine.Invocation;
using clicb = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class MemoryCommand : Command
    {
        public MemoryCommand()
            : base("memory", "Get memory info from rdb files")
        {
            var arg = new Argument<string>("file", "The path of rdb files");

            this.AddArgument(arg);

            this.SetHandler((InvocationContext context) =>
            {
                var files = context.ParseResult.GetValueForArgument<string>(arg);
                Do(context, files);

                context.Console.WriteLine($"");
            });

        }

        private void Do(InvocationContext context, string files)
        {
            var console = context.Console;
            var cb = new clicb.MemoryCallback();

            console.WriteLine($"");
            console.WriteLine($"Find keys in [{files}] are as follow:");
            console.WriteLine($"");

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(files);

            var rdbDataInfo = cb.GetRdbDataInfo();

            var str = System.Text.Json.JsonSerializer.Serialize(rdbDataInfo, new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true,
            });

            console.WriteLine($"{str}");
        }
    }
}

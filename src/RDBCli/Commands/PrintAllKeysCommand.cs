using System.CommandLine;
using System.CommandLine.Invocation;
using clicb = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class PrintAllKeysCommand : Command
    {
        public PrintAllKeysCommand() 
            : base("keys", "Get all keys from rdb files")
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
            var cb = new clicb.KeysOnlyCallback(console);

            console.WriteLine($"");
            console.WriteLine($"Find keys in [{files}] are as follow:");
            console.WriteLine($"");

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(files);
        }
    }
}

using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;

namespace RDBCli.Commands
{
    internal class TestCommand : Command
    {
        public TestCommand()
            : base("test", "Try to parser rdb files without operation")
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
            var cb = new RDBParser.NoOpReaderCallBack();

            console.WriteLine($"");
            console.WriteLine($"Prepare to parse [{files}]");
            console.WriteLine($"Please wait for a moment...\n");

            var sw = new Stopwatch();
            sw.Start();

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(files);

            sw.Stop();
            console.WriteLine($"parse cost: {sw.ElapsedMilliseconds}ms");
        }
    }
}

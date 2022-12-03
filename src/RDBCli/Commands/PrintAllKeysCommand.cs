using System.Collections.Generic;
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
            var keyPrefixOption =
                new Option<List<string>>(
                    aliases: new string[] { "--key-prefix" },
                    description: "The filter of redis key prefix.");

            this.AddArgument(arg);
            this.AddOption(keyPrefixOption);

            this.SetHandler((InvocationContext context) => 
            {
                var files = context.ParseResult.GetValueForArgument<string>(arg);
                var keyPrefixes = context.ParseResult.GetValueForOption<List<string>>(keyPrefixOption);
                Do(context, files, keyPrefixes);

                context.Console.WriteLine($"");
            });

        }

        private void Do(InvocationContext context, string files, List<string> keyPrefixes)
        {
            var console = context.Console;
            var cb = new clicb.KeysOnlyCallback(console, keyPrefixes);

            console.WriteLine($"");
            console.WriteLine($"Find keys in [{files}] are as follow:");
            console.WriteLine($"");

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(files);
        }
    }
}

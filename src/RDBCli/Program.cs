using System;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Help;
using System.CommandLine.Parsing;
using System.Threading.Tasks;

namespace RDBCli
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var root = new RootCommand
            {
                new Commands.PrintAllKeysCommand(),
                new Commands.MemoryCommand(),
                new Commands.TestCommand(),
                new Commands.CsvCommand(),

            };

            root.Description = "rdb-cli is a command line tool, analysis redis/valkey rdb files.";

            var parser = new CommandLineBuilder(root)
                .UseHelp()
                .UseVersionOption(new[] { "-v", "--version" })
                .UseSuggestDirective()
                .UseParseErrorReporting()
                .UseExceptionHandler()
                .Build();

            if (args.Length == 0)
            {
                var helpBuilder = new HelpBuilder(LocalizationResources.Instance, Console.WindowWidth);
                helpBuilder.Write(root, Console.Out);
                return 0;
            }

            return await parser.InvokeAsync(args);
        }
    }
}

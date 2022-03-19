using System.CommandLine;
using System.CommandLine.Invocation;

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
                context.Console.WriteLine($"Begin print all keys command");

                var files = context.ParseResult.GetValueForArgument<string>(arg);

                Do(files);

                context.Console.WriteLine($"End print all keys command");
            });

        }

        private void Do(string files)
        {
        }
    }
}

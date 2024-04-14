using System.CommandLine;

namespace RDBCli.Commands
{
    internal static class CommonCLIArguments
    {
        public static Argument<string> FileArgument()
        {
            Argument<string> arg = 
                new Argument<string>("file", "The path of rdb file.");

            return arg;
        }
    }
}

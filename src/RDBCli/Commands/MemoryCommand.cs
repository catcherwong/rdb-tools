using System.CommandLine;
using System.CommandLine.Invocation;
using System.Linq;
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
            var rdbDataInfo = cb.GetRdbDataInfo();

            var counter = new RdbDataCounter(rdbDataInfo.Records);
            counter.Count();

            console.WriteLine($"");
            console.WriteLine($"Find keys in [{files}] are as follow:");
            console.WriteLine($"");

            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(files);

            sw.Stop();
            console.WriteLine($"parse cost: {sw.ElapsedMilliseconds}ms");
            
            var options = new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true,
            };

            rdbDataInfo.Records = null;
            var str0 = System.Text.Json.JsonSerializer.Serialize(rdbDataInfo, options);
            console.WriteLine($"{str0}");

            var str = System.Text.Json.JsonSerializer.Serialize(counter.TypeNum, options);
            console.WriteLine($"{str}");

            //var tmp = counter.KeyPrefixNum
            //    .Select(x => new { k = x.Key.ToString(), v = x.Value })
            //    .OrderByDescending(x => x.v)
            //    .ToList();

            console.WriteLine(counter.KeyPrefixNum.Keys.Count.ToString());            
        }
    }
}

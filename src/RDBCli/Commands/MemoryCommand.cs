using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using clicb = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class MemoryCommand : Command
    {
        public MemoryCommand()
            : base("memory", "Get memory info from rdb files")
        {
            var arg = new Argument<string>("file", "The path of rdb files.");

            var outputOption = new Option<string>("--output", "The output path of parsing result.");

            this.AddOption(outputOption);
            this.AddArgument(arg);
            
            this.SetHandler((InvocationContext context) =>
            {
                var files = context.ParseResult.GetValueForArgument<string>(arg);
                var output = context.ParseResult.GetValueForOption<string>(outputOption);

                Do(context, files, output);

                context.Console.WriteLine($"");
            });
        }

        private void Do(InvocationContext context, string files, string output)
        {
            var console = context.Console;
            var cb = new clicb.MemoryCallback();
            var rdbDataInfo = cb.GetRdbDataInfo();

            var counter = new RdbDataCounter(rdbDataInfo.Records);
            counter.Count();

            console.WriteLine($"");
            console.WriteLine($"Prepare to parse [{files}]");
            console.WriteLine($"Please wait for a moment...\n");

            var sw = new Stopwatch();
            sw.Start();

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(files);

            sw.Stop();
            console.WriteLine($"parse cost: {sw.ElapsedMilliseconds}ms");
            sw.Start();

            var largestRecords = counter.GetLargestRecords(10);
            var largestKeyPrefix = counter.GetLargestKeyPrefixes(10);
            var typeRecords = counter.GetTypeRecords();
            var expiryInfo = counter.GetExpiryInfo();

            var dict = new Dictionary<string, object>();

            dict.Add("usedMem", rdbDataInfo.UsedMem);
            dict.Add("cTime", rdbDataInfo.CTime);
            dict.Add("count", rdbDataInfo.Count);
            dict.Add("rdbVer", rdbDataInfo.RdbVer);
            dict.Add("redisVer", rdbDataInfo.RedisVer);
            dict.Add("redisBits", rdbDataInfo.RedisBits);
            dict.Add("typeRecords", typeRecords);
            dict.Add("largestRecords", largestRecords);
            dict.Add("largestKeyPrefix", largestKeyPrefix);
            dict.Add("expiryInfo", expiryInfo);

            var path = WriteJsonFile(dict, output);

            sw.Stop();
            console.WriteLine($"total cost: {sw.ElapsedMilliseconds}ms");
            console.WriteLine($"result path: {path}");
        }

        private string WriteJsonFile(Dictionary<string, object> dict, string output)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };

            var stream = JsonSerializer.SerializeToUtf8Bytes(dict, options);

            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "res.json");

            if (!string.IsNullOrWhiteSpace(output)) path = output;

            using FileStream fs = new(path, FileMode.Create, FileAccess.Write);
            fs.Write(stream, 0, stream.Length);

            return path;
        }
    }
}

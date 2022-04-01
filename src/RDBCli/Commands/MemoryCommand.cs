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
        public static Argument<string> Arg = new Argument<string>("file", "The path of rdb file.");
        // TODO: check output path valid or not
        public static Option<string> OutputOption = new Option<string>(new string[] { "--output", "-o" }, "The output path of parsing result.");
        public static Option<string> OutputTypeOption = new Option<string>(new string[] { "--output-type", "-ot" }, () => "json", "The output type of parsing result.").FromAmong("json", "html");
        public static Option<int> TopPrefixCountOption = new Option<int>(new string[] { "--top-prefixes", "-tp" }, () => 50, "The number of top key prefixes.");
        public static Option<int> TopBigKeyCountOption = new Option<int>(new string[] { "--top-bigkeys", "-tb" }, () => 50, "The number of top big keys.");

        public MemoryCommand()
            : base("memory", "Get memory info from rdb files")
        {
            TopPrefixCountOption.AddValidator(x => 
            {
                var c = x.GetValueOrDefault<int>();
                if (c > 200) x.ErrorMessage = "The number can not greater than 200!!";
            });

            TopBigKeyCountOption.AddValidator(x =>
            {
                var c = x.GetValueOrDefault<int>();
                if (c > 200) x.ErrorMessage = "The number can not greater than 200!!";
            });

            this.AddOption(OutputOption);
            this.AddOption(OutputTypeOption);
            this.AddOption(TopPrefixCountOption);
            this.AddOption(TopBigKeyCountOption);
            this.AddArgument(Arg);
            
            this.SetHandler((InvocationContext context) =>
            {
                var opt = CommandOptions.FromContext(context);

                Do(context, opt);
            });
        }

        private void Do(InvocationContext context, CommandOptions options)
        {
            var console = context.Console;
            var cb = new clicb.MemoryCallback();
            var rdbDataInfo = cb.GetRdbDataInfo();

            var counter = new RdbDataCounter(rdbDataInfo.Records);
            counter.Count();

            console.WriteLine($"");
            console.WriteLine($"Prepare to parse [{options.Files}]");
            console.WriteLine($"Please wait for a moment...\n");

            var sw = new Stopwatch();
            sw.Start();

            var parser = new RDBParser.BinaryReaderRDBParser(cb);
            parser.Parse(options.Files);

            sw.Stop();
            console.WriteLine($"parse cost: {sw.ElapsedMilliseconds}ms");
            sw.Start();

            var largestRecords = counter.GetLargestRecords(options.TopBigKeyCount);
            var largestKeyPrefix = counter.GetLargestKeyPrefixes(options.TopPrefixCount);
            var typeRecords = counter.GetTypeRecords();
            var expiryInfo = counter.GetExpiryInfo();

            var dict = new Dictionary<string, object>();

            dict.Add("usedMem", rdbDataInfo.UsedMem > 0 ? rdbDataInfo.UsedMem : rdbDataInfo.TotalMem);
            dict.Add("cTime", rdbDataInfo.CTime);
            dict.Add("count", rdbDataInfo.Count);
            dict.Add("rdbVer", rdbDataInfo.RdbVer);
            dict.Add("redisVer", string.IsNullOrWhiteSpace(rdbDataInfo.RedisVer) ? CommonHelper.GetFuzzyRedisVersion(rdbDataInfo.RdbVer) : rdbDataInfo.RedisVer);
            dict.Add("redisBits", rdbDataInfo.RedisBits);
            dict.Add("typeRecords", typeRecords);
            dict.Add("largestRecords", largestRecords);
            dict.Add("largestKeyPrefix", largestKeyPrefix);
            dict.Add("expiryInfo", expiryInfo);

            var path = options.OutputType == "json"
                ? WriteJsonFile(dict, options.Output)
                : WriteHtmlFile(dict, options.Output);

            sw.Stop();
            console.WriteLine($"total cost: {sw.ElapsedMilliseconds}ms");
            console.WriteLine($"result path: {path}\n");
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

        private string WriteHtmlFile(Dictionary<string, object> dict, string output)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };

            var str = JsonSerializer.Serialize(dict, options);

            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "res.html");

            if (!string.IsNullOrWhiteSpace(output)) path = output;

            var tplStream = this.GetType().Assembly.GetManifestResourceStream("RDBCli.Tpl.tpl.html");
            var tpl = string.Empty;

            using var reader = new StreamReader(tplStream, System.Text.Encoding.UTF8);
            tpl = reader.ReadToEnd();
            tpl = tpl.Replace("{{CLIDATA}}", str);

            var bytes = System.Text.Encoding.UTF8.GetBytes(tpl);
            using FileStream fs = new(path, FileMode.Create, FileAccess.Write);
            fs.Write(bytes, 0, bytes.Length);

            return path;
        }

        private class CommandOptions
        {
            public string Files { get; set; }
            public string Output { get; set; }
            public string OutputType { get; set; }
            public int TopPrefixCount { get; set; }
            public int TopBigKeyCount { get; set; }

            public static CommandOptions FromContext(InvocationContext context)
            {
                var files = context.ParseResult.GetValueForArgument<string>(Arg);
                var output = context.ParseResult.GetValueForOption<string>(OutputOption);
                var outputType = context.ParseResult.GetValueForOption<string>(OutputTypeOption);
                var pc = context.ParseResult.GetValueForOption<int>(TopPrefixCountOption);
                var bc = context.ParseResult.GetValueForOption<int>(TopBigKeyCountOption);

                return new CommandOptions
                {
                    Files = files,
                    Output = output,
                    OutputType = outputType,
                    TopBigKeyCount = bc,
                    TopPrefixCount = pc,
                };
            }
        }
    }
}

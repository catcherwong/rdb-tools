using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using clicb = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class MemoryCommand : Command
    {
        public static Argument<string> Arg = new Argument<string>("file", "The path of rdb file.");
        public static Option<string> OutputOption = new Option<string>(new string[] { "--output", "-o" }, "The output path of parsing result.").LegalFilePathsOnly();
        public static Option<string> OutputTypeOption = new Option<string>(new string[] { "--output-type", "-ot" }, () => "json", "The output type of parsing result.").FromAmong("json", "html");
        public static Option<int> TopPrefixCountOption = new Option<int>(new string[] { "--top-prefixes", "-tp" }, () => 50, "The number of top key prefixes.");
        public static Option<int> TopBigKeyCountOption = new Option<int>(new string[] { "--top-bigkeys", "-tb" }, () => 50, "The number of top big keys.");
        public static Option<List<int>> DBsOption = new Option<List<int>>(new string[] { "--db" }, "The redis databases.");
        public static Option<List<string>> TypesOption = new Option<List<string>>(new string[] { "--type" }, "The redis types.");

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
            this.AddOption(DBsOption);
            this.AddOption(TypesOption);
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
            var task = counter.Count();

            console.WriteLine($"");
            console.WriteLine($"Prepare to parse [{options.Files}]");
            console.WriteLine($"Please wait for a moment...\n");

            var sw = new Stopwatch();
            sw.Start();

            var parser = new RDBParser.BinaryReaderRDBParser(cb, options.ParserFilter);
            parser.Parse(options.Files);

            sw.Stop();
            console.WriteLine($"parse cost: {sw.ElapsedMilliseconds}ms");
            sw.Start();

            task.Wait();

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

            var path = WriteFile(dict, options.Output, options.OutputType);

            sw.Stop();
            console.WriteLine($"total cost: {sw.ElapsedMilliseconds}ms");
            console.WriteLine($"result path: {path}\n");
        }

        private string WriteFile(Dictionary<string, object> dict, string output, string type)
        {
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"res.{type}");

            if (!string.IsNullOrWhiteSpace(output))
            {
                if (!Directory.Exists(output)) Directory.CreateDirectory(output);
                path = Path.Combine(output, $"res.{type}");
            }

            byte[] bytes = null;

            if (type.Equals("json"))
            {
                bytes = JsonSerializer.SerializeToUtf8Bytes(dict, _jsonOptions);
            }
            else if (type.Equals("html"))
            {
                var str = JsonSerializer.Serialize(dict, _jsonOptions);
                var tpl = CommonHelper.TplHtmlString;
                tpl = tpl.Replace("{{CLIDATA}}", str);

                bytes = System.Text.Encoding.UTF8.GetBytes(tpl);
            }
            else
            {
                bytes = new byte[1];
            }

            using FileStream fs = new(path, FileMode.Create, FileAccess.Write);
            fs.Write(bytes, 0, bytes.Length);

            return path;
        }

        private static JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            // WriteIndented = true,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private class CommandOptions
        {
            public string Files { get; set; }
            public string Output { get; set; }
            public string OutputType { get; set; }
            public int TopPrefixCount { get; set; }
            public int TopBigKeyCount { get; set; }
            public RDBParser.ParserFilter ParserFilter { get; set; }

            public static CommandOptions FromContext(InvocationContext context)
            {
                var files = context.ParseResult.GetValueForArgument<string>(Arg);
                var output = context.ParseResult.GetValueForOption<string>(OutputOption);
                var outputType = context.ParseResult.GetValueForOption<string>(OutputTypeOption);
                var pc = context.ParseResult.GetValueForOption<int>(TopPrefixCountOption);
                var bc = context.ParseResult.GetValueForOption<int>(TopBigKeyCountOption);
                var databases = context.ParseResult.GetValueForOption<List<int>>(DBsOption);
                var types = context.ParseResult.GetValueForOption<List<string>>(TypesOption);

                var parseFilter = new RDBParser.ParserFilter()
                {
                    Databases = databases,
                    Types = types
                };
                
                return new CommandOptions
                {
                    Files = files,
                    Output = output,
                    OutputType = outputType,
                    TopBigKeyCount = bc,
                    TopPrefixCount = pc,
                    ParserFilter = parseFilter
                };
            }
        }
    }
}

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
            var outputTypeOption = new Option<string>("--output-type", () => "json", "The output type of parsing result.").FromAmong("json", "html");

            this.AddOption(outputOption);
            this.AddOption(outputTypeOption);
            this.AddArgument(arg);
            
            this.SetHandler((InvocationContext context) =>
            {
                var files = context.ParseResult.GetValueForArgument<string>(arg);
                var output = context.ParseResult.GetValueForOption<string>(outputOption);
                var outputType = context.ParseResult.GetValueForOption<string>(outputTypeOption);

                Do(context, files, output, outputType);

                context.Console.WriteLine($"");
            });
        }

        private void Do(InvocationContext context, string files, string output, string outputType)
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

            var largestRecords = counter.GetLargestRecords(50);
            var largestKeyPrefix = counter.GetLargestKeyPrefixes(50);
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

            var path = string.Empty;

            if (outputType == "json")
            {
                path = WriteJsonFile(dict, output);
            }
            else
            { 
                path = WriteHtmlFile(dict, output);
            }

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
    }
}

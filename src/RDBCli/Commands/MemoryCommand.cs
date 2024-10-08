﻿using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using CliCB = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class MemoryCommand : Command
    {
        private static Option<string> _outputOption = CommonCLIOptions.OutputOption();
        private static Option<string> _outputTypeOption = CommonCLIOptions.OutputTypeOption();
        private static Option<int> _topPrefixCountOption = CommonCLIOptions.TopPrefixCountOption();
        private static Option<int> _topBigKeyCountOption = CommonCLIOptions.TopBigKeyCountOption();
        private static Option<List<int>> _databasesOption = CommonCLIOptions.DBsOption();
        private static Option<List<string>> _typesOption = CommonCLIOptions.TypesOption();
        private static Option<List<string>> _keyPrefixesOption = CommonCLIOptions.KeyPrefixesOption();
        private static Option<string> _separatorsOption = CommonCLIOptions.SeparatorsOption();
        private static Option<int> _sepPrefixCountOption = CommonCLIOptions.SepPrefixCountOption();
        private static Option<bool?> _isPermanentOption = CommonCLIOptions.IsPermanentOption();
        private static Option<bool?> _isIgnoreFieldOfLargestElemOption = CommonCLIOptions.IsIgnoreFieldOfLargestElemOption();
        private static Option<bool?> _keySuffixEnableOption = CommonCLIOptions.KeySuffixEnableOption();
        private static Option<string> _cdnOption = CommonCLIOptions.CDNOption();
        private static Argument<string> _fileArg = CommonCLIArguments.FileArgument();

        public MemoryCommand()
            : base("memory", "Analysis memory info from rdb files")
        {
            this.AddOption(_outputOption);
            this.AddOption(_outputTypeOption);
            this.AddOption(_topPrefixCountOption);
            this.AddOption(_topBigKeyCountOption);
            this.AddOption(_databasesOption);
            this.AddOption(_typesOption);
            this.AddOption(_keyPrefixesOption);
            this.AddOption(_separatorsOption);
            this.AddOption(_sepPrefixCountOption);
            this.AddOption(_isPermanentOption);
            this.AddOption(_isIgnoreFieldOfLargestElemOption);
            this.AddOption(_keySuffixEnableOption);
            this.AddOption(_cdnOption);
            this.AddArgument(_fileArg);

            this.SetHandler((InvocationContext context) =>
            {
                var opt = CommandOptions.FromContext(context);

                Do(context, opt);
            });
        }

        private void Do(InvocationContext context, CommandOptions options)
        {
            var console = context.Console;
            var cb = new CliCB.MemoryCallback(options.IsIgnoreFole ?? false);
            //var rdbDataInfo = cb.GetRdbDataInfo();

            var counter = new RdbDataCounter(cb, options.Separators, options.SepPrefixCount, options.keySuffixEnable ?? false);
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
            var streamRecords = counter.GetStreamRecords();
            var idleOrFreqInfo = counter.GetIdleOrFreqInfo();
            var dbInfo = counter.GetDatabaseInfo();

            var dict = MemoryAnslysisResult.BuildBasicFromRdbDataInfo(cb.GetRdbDataInfo());
            dict.typeRecords = typeRecords;
            dict.largestKeyPrefix = largestKeyPrefix;
            dict.largestRecords = largestRecords;
            dict.expiryInfo = expiryInfo;
            dict.largestStreams = streamRecords;
            dict.idleOrFreqInfo = idleOrFreqInfo;
            dict.dbInfo = dbInfo;

            var exp = expiryInfo.FirstOrDefault(x => x.Expiry.Equals(CommonHelper.AlreadyExpired));
            var perm = expiryInfo.FirstOrDefault(x => x.Expiry.Equals(CommonHelper.Permanent));
            dict.expCount = (int)(exp?.Num ?? 0);
            dict.expMem = (long)(exp?.Bytes ?? 0);
            dict.permCount = (int)(perm?.Num ?? 0);
            dict.permMem = (long)(perm?.Bytes ?? 0);

            var path = WriteFile(dict, options.Output, options.OutputType, options.CDN);

            sw.Stop();
            console.WriteLine($"total cost: {sw.ElapsedMilliseconds}ms");
            console.WriteLine($"result path: {path}\n");
        }

        private string WriteFile(MemoryAnslysisResult dict, string output, string type, string cdn)
        {
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"res.{type}");

            // the output contains file name with specific type
            if (!string.IsNullOrWhiteSpace(output))
            {
                if (output.EndsWith(type, StringComparison.OrdinalIgnoreCase))
                {
                    var dir = Path.GetDirectoryName(output);
                    if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                    path = output;
                }
                else
                {
                    if (!Directory.Exists(output)) Directory.CreateDirectory(output);
                    path = Path.Combine(output, $"res.{type}");
                }
            }

            byte[] bytes = null;

            var context = new MemoryAnslysisResultJsonContext(_jsonOptions);

            if (type.Equals("json"))
            {
                bytes = JsonSerializer.SerializeToUtf8Bytes(dict, typeof(MemoryAnslysisResult), context);
            }
            else if (type.Equals("html"))
            {
                var str = JsonSerializer.Serialize(dict, typeof(MemoryAnslysisResult), context);
                var tpl = CommonHelper.TplHtmlString;
                tpl = tpl.Replace("{{CDNDOMAIN}}", cdn);
                tpl = tpl.Replace("{{CLIDATA}}", str);

                bytes = System.Text.Encoding.UTF8.GetBytes(tpl);
            }
            else if (type.Equals("csv"))
            {
                // csv only output largestRecords or multi csv files with full MemoryAnslysisResult?
                var builder = new System.Text.StringBuilder();
                builder.AppendLine("database,type,key,size_in_bytes,encoding,num_elements,len_largest_element,expiry");
                foreach (var item in dict.largestRecords)
                {
                    builder.AppendLine($"{item.Database},{item.Type},{item.Key},{item.Bytes},{item.Encoding},{item.NumOfElem},{item.LenOfLargestElem},{CommonHelper.GetExpireString(item.Expiry)}");
                }

                bytes = System.Text.Encoding.UTF8.GetBytes(builder.ToString());
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
            public string Separators { get; set; }
            public int SepPrefixCount { get; set; }
            public bool? IsIgnoreFole { get; set; }
            public bool? keySuffixEnable { get; set; }
            public string CDN { get; set; }


            public static CommandOptions FromContext(InvocationContext context)
            {
                var files = context.ParseResult.GetValueForArgument<string>(_fileArg);
                var output = context.ParseResult.GetValueForOption<string>(_outputOption);
                var outputType = context.ParseResult.GetValueForOption<string>(_outputTypeOption);
                var pc = context.ParseResult.GetValueForOption<int>(_topPrefixCountOption);
                var bc = context.ParseResult.GetValueForOption<int>(_topBigKeyCountOption);
                var databases = context.ParseResult.GetValueForOption<List<int>>(_databasesOption);
                var types = context.ParseResult.GetValueForOption<List<string>>(_typesOption);
                var keyPrefixes = context.ParseResult.GetValueForOption<List<string>>(_keyPrefixesOption);
                var sep = context.ParseResult.GetValueForOption<string>(_separatorsOption);
                var sepPrefixCount = context.ParseResult.GetValueForOption<int>(_sepPrefixCountOption);
                var isPermanent = context.ParseResult.GetValueForOption<bool?>(_isPermanentOption);
                var isIgnoreFole = context.ParseResult.GetValueForOption<bool?>(_isIgnoreFieldOfLargestElemOption);
                var keySuffixEnable = context.ParseResult.GetValueForOption<bool?>(_keySuffixEnableOption);
                var cdn = context.ParseResult.GetValueForOption<string>(_cdnOption);

                var parseFilter = new RDBParser.ParserFilter()
                {
                    Databases = databases,
                    Types = types,
                    KeyPrefixes = keyPrefixes,
                    IsPermanent = isPermanent
                };

                return new CommandOptions
                {
                    Files = files,
                    Output = output,
                    OutputType = outputType,
                    TopBigKeyCount = bc,
                    TopPrefixCount = pc,
                    ParserFilter = parseFilter,
                    Separators = sep,
                    SepPrefixCount = sepPrefixCount,
                    IsIgnoreFole = isIgnoreFole,
                    keySuffixEnable = keySuffixEnable,
                    CDN = cdn,
                };
            }
        }
    }

    public class MemoryAnslysisResult
    {
        public long usedMem { get; set; }
        public long cTime { get; set; }
        public int count { get; set; }
        public int rdbVer { get; set; }
        public string redisVer { get; set; }
        public string redisType { get; set; }
        public long redisBits { get; set; }
        public int expCount { get; set; }
        public int permCount { get; set; }
        public long expMem { get; set; }
        public long permMem { get; set; }
        public List<TypeRecord> typeRecords { get; set; }
        public List<Record> largestRecords { get; set; }
        public List<PrefixRecord> largestKeyPrefix { get; set; }
        public List<ExpiryRecord> expiryInfo { get; set; }
        public List<IdleOrFreqRecord> idleOrFreqInfo { get; set; }
        public List<FunctionsRecord> functions { get; set; }
        public List<StreamsRecord> largestStreams { get; set; }
        public List<DBRecord> dbInfo { get; set; }

        internal static MemoryAnslysisResult BuildBasicFromRdbDataInfo(RdbDataInfo rdbDataInfo)
        {
            var result = new MemoryAnslysisResult
            {
                usedMem = rdbDataInfo.UsedMem > 0 ? rdbDataInfo.UsedMem : (long)rdbDataInfo.TotalMem,
                cTime = rdbDataInfo.CTime,
                count = rdbDataInfo.Count,
                rdbVer = rdbDataInfo.RdbVer,
                redisVer = string.IsNullOrWhiteSpace(rdbDataInfo.RedisVer) ? CommonHelper.GetFuzzyRedisVersion(rdbDataInfo.RdbVer) : rdbDataInfo.RedisVer,
                redisBits = rdbDataInfo.RedisBits,
                functions = rdbDataInfo.Functions,
                redisType = rdbDataInfo.RedisType,
            };

            return result;
        }
    }

    [System.Text.Json.Serialization.JsonSerializable(typeof(MemoryAnslysisResult))]
    internal partial class MemoryAnslysisResultJsonContext : System.Text.Json.Serialization.JsonSerializerContext
    {
    }
}

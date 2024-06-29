using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CliCB = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class CsvCommand : Command
    {
        private static Option<string> _outputOption = CommonCLIOptions.OutputOption();
        private static Option<List<int>> _databasesOption = CommonCLIOptions.DBsOption();
        private static Option<List<string>> _typesOption = CommonCLIOptions.TypesOption();
        private static Option<List<string>> _keyPrefixesOption = CommonCLIOptions.KeyPrefixesOption();
        private static Option<ulong?> _minIdleOption = CommonCLIOptions.MinIdleOption();
        private static Option<int?> _minFreqOption = CommonCLIOptions.MinFreqOption();
        private static Option<string> _separatorsOption = CommonCLIOptions.SeparatorsOption();
        private static Option<int> _sepPrefixCountOption = CommonCLIOptions.SepPrefixCountOption();
        private static Option<bool?> _permanentOption = CommonCLIOptions.IsPermanentOption();
        private static Option<bool?> _expiredOption = CommonCLIOptions.IsExpiredOption();
        private static Option<bool?> _keySuffixEnableOption = CommonCLIOptions.KeySuffixEnableOption();
        private static Argument<string> _fileArg = CommonCLIArguments.FileArgument();

        public CsvCommand()
            : base("csv", "Convert rdb file to csv.")
        {
            this.AddOption(_outputOption);
            this.AddOption(_databasesOption);
            this.AddOption(_typesOption);
            this.AddOption(_keyPrefixesOption);
            this.AddOption(_minIdleOption);
            this.AddOption(_minFreqOption);
            this.AddOption(_separatorsOption);
            this.AddOption(_sepPrefixCountOption);
            this.AddOption(_keySuffixEnableOption);
            this.AddOption(_permanentOption);
            this.AddOption(_expiredOption);
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
            var cb = new CliCB.MemoryCallback();
            var rdbDataInfo = cb.GetRdbDataInfo();

            var counter = new RdbCsvData(rdbDataInfo.Records, options.Separators, options.SepPrefixCount, options.keySuffixEnable);
            var task = counter.Output(options.Output);

            console.WriteLine($"");
            console.WriteLine($"Prepare to parse [{options.Files}]");
            console.WriteLine($"Please wait for a moment...\n");

            var sw = new Stopwatch();
            sw.Start();

            var parser = new RDBParser.BinaryReaderRDBParser(cb, options.ParserFilter);
            parser.Parse(options.Files);

            sw.Stop();

            task.Wait();

            console.WriteLine($"total cost: {sw.ElapsedMilliseconds}ms");
            console.WriteLine($"result path: {task.Result}\n");
        }

        private class CommandOptions
        {
            public string Files { get; set; }
            public string Output { get; set; }
            public string Separators { get; set; }
            public int SepPrefixCount { get; set; }
            public bool keySuffixEnable { get; set; }
            public RDBParser.ParserFilter ParserFilter { get; set; }

            public static CommandOptions FromContext(InvocationContext context)
            {
                var files = context.ParseResult.GetValueForArgument<string>(_fileArg);
                var output = context.ParseResult.GetValueForOption<string>(_outputOption);
                var databases = context.ParseResult.GetValueForOption<List<int>>(_databasesOption);
                var types = context.ParseResult.GetValueForOption<List<string>>(_typesOption);
                var keyPrefixes = context.ParseResult.GetValueForOption<List<string>>(_keyPrefixesOption);
                var minIdle = context.ParseResult.GetValueForOption<ulong?>(_minIdleOption);
                var minFreq = context.ParseResult.GetValueForOption<int?>(_minFreqOption);
                var sep = context.ParseResult.GetValueForOption<string>(_separatorsOption);
                var sepPrefixCount = context.ParseResult.GetValueForOption<int>(_sepPrefixCountOption);
                var keySuffixEnable = context.ParseResult.GetValueForOption<bool?>(_keySuffixEnableOption);
                var permanent = context.ParseResult.GetValueForOption<bool?>(_permanentOption);
                var expired = context.ParseResult.GetValueForOption<bool?>(_expiredOption);

                var parseFilter = new RDBParser.ParserFilter()
                {
                    Databases = databases,
                    Types = types,
                    KeyPrefixes = keyPrefixes,
                    MinFreq = minFreq,
                    MinIdle = minIdle,
                    IsPermanent = permanent,
                    IsExpired = expired
                };

                return new CommandOptions
                {
                    Files = files,
                    Output = output,
                    ParserFilter = parseFilter,
                    Separators = sep,
                    SepPrefixCount = sepPrefixCount,
                    keySuffixEnable = keySuffixEnable ?? false
                };
            }
        }
    }

    internal class RdbCsvData
    {
        private BlockingCollection<AnalysisRecord> _records;
        private readonly char[] _separators;
        private readonly int _sepCount;
        private readonly bool _keySuffixEnable;
        private Dictionary<string, (long, long)> _prefixDict = new Dictionary<string, (long, long)>();
        public RdbCsvData(BlockingCollection<AnalysisRecord> records, string separators = "", int sepCount = -1, bool keySuffixEnable = false)
        {
            this._records = records;
            if (!string.IsNullOrWhiteSpace(separators))
            {
                this._separators = separators.ToCharArray();
            }
            this._sepCount = sepCount > 0 ? sepCount : 1;
            this._keySuffixEnable = keySuffixEnable;
        }

        public Task<string> Output(string output)
        {
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"dump.csv");
            if (!string.IsNullOrWhiteSpace(output))
            {
                if(output.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                {
                    var dir = Path.GetDirectoryName(output);
                    if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                    path = output;
                }
                else
                {
                    if (!Directory.Exists(output)) Directory.CreateDirectory(output);
                    path = Path.Combine(output, $"dump.csv");
                }
            }

            System.Threading.CancellationTokenSource cts = new System.Threading.CancellationTokenSource();
            var task = Task.Factory.StartNew(() =>
            {
                if (_separators != null && _separators.Any())
                {
                    while (!_records.IsCompleted)
                    {
                        if (_records.TryTake(out var item))
                        {
                            var prefixs = CommonHelper.GetPrefixes(item.Record.Key, _separators, _sepCount, _keySuffixEnable);

                            foreach (var p in prefixs)
                            {
                                if (_prefixDict.TryGetValue($"{item.Record.Database}!{item.Record.Type}!{p}", out var v))
                                {
                                    v.Item1 += (long)item.Record.Bytes;
                                    v.Item2 += 1;
                                }
                                else
                                {
                                    _prefixDict.Add($"{item.Record.Database}!{item.Record.Type}!{p}", ((long)item.Record.Bytes, 1));
                                }
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }

                    try
                    {
                        using (FileStream fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                        {
                            // overwrite
                            fs.SetLength(0);
                            var header = Encoding.UTF8.GetBytes("database,type,key_prefix,size_in_bytes,count\n");
                            fs.Write(header);

                            foreach (var item in _prefixDict)
                            {
                                var keys = item.Key.Split('!');
                                var line = Encoding.UTF8.GetBytes($"{keys[0]},{keys[1]},{keys[2]},{item.Value.Item1},{item.Value.Item2}\n");
                                fs.Write(line);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                {
                    using (FileStream fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                    {
                        // overwrite
                        fs.SetLength(0);
                        var header = Encoding.UTF8.GetBytes("database,type,key,size_in_bytes,encoding,num_elements,len_largest_element,expiry,idle,freq\n");
                        fs.Write(header);

                        while (!_records.IsCompleted)
                        {
                            try
                            {
                                if (_records.TryTake(out var item))
                                {
                                    var line = Encoding.UTF8.GetBytes($"{item.Record.Database},{item.Record.Type},{item.Record.Key},{item.Record.Bytes},{item.Record.Encoding},{item.Record.NumOfElem},{item.Record.LenOfLargestElem},{CommonHelper.GetExpireString(item.Record.Expiry)},{item.Record.Idle},{item.Record.Freq}\n");
                                    fs.Write(line);
                                }
                                else
                                {
                                    continue;
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }
                }

                cts.Cancel();

                return path;
            }, cts.Token);

            return task;
        }
    }
}

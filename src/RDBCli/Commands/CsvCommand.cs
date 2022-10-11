using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using clicb = RDBCli.Callbacks;

namespace RDBCli.Commands
{
    internal class CsvCommand : Command
    {
        private static Option<string> _outputOption = CommonCLIOptions.OutputOption();
        private static Option<List<int>> _databasesOption = CommonCLIOptions.DBsOption();
        private static Option<List<string>> _typesOption = CommonCLIOptions.TypesOption();
        private static Option<List<string>> _keyPrefixesOption = CommonCLIOptions.KeyPrefixesOption();
        private static Argument<string> _fileArg = CommonCLIArguments.FileArgument();

        public CsvCommand()
            : base("csv", "Convert rdb file to csv.")
        {
            this.AddOption(_outputOption);
            this.AddOption(_databasesOption);
            this.AddOption(_typesOption);
            this.AddOption(_keyPrefixesOption);
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
            var cb = new clicb.MemoryCallback();
            var rdbDataInfo = cb.GetRdbDataInfo();

            var counter = new RdbCsvData(rdbDataInfo.Records);
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
            public RDBParser.ParserFilter ParserFilter { get; set; }

            public static CommandOptions FromContext(InvocationContext context)
            {
                var files = context.ParseResult.GetValueForArgument<string>(_fileArg);
                var output = context.ParseResult.GetValueForOption<string>(_outputOption);
                var databases = context.ParseResult.GetValueForOption<List<int>>(_databasesOption);
                var types = context.ParseResult.GetValueForOption<List<string>>(_typesOption);
                var keyPrefixes = context.ParseResult.GetValueForOption<List<string>>(_keyPrefixesOption);

                var parseFilter = new RDBParser.ParserFilter()
                {
                    Databases = databases,
                    Types = types,
                    KeyPrefixes = keyPrefixes,
                };

                return new CommandOptions
                {
                    Files = files,
                    Output = output,
                    ParserFilter = parseFilter
                };
            }
        }
    }

    internal class RdbCsvData
    {
        private BlockingCollection<AnalysisRecord> _records;

        public RdbCsvData(BlockingCollection<AnalysisRecord> records)
        {
            this._records = records;
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
                using (FileStream fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                {
                    // overwrite
                    fs.SetLength(0);
                    var header = Encoding.UTF8.GetBytes("database,type,key,size_in_bytes,encoding,num_elements,len_largest_element,expiry\n");
                    fs.Write(header);

                    while (!_records.IsCompleted)
                    {
                        try
                        {
                            if (_records.TryTake(out var item))
                            {
                                var line = Encoding.UTF8.GetBytes($"{item.Record.Database},{item.Record.Type},{item.Record.Key},{item.Record.Bytes},{item.Record.Encoding},{item.Record.NumOfElem},{item.Record.LenOfLargestElem},{CommonHelper.GetExpireString(item.Record.Expiry)}\n");
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

                cts.Cancel();

                return path;
            }, cts.Token);

            return task;
        }
    }
}

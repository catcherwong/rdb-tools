using System.Collections.Generic;
using System.CommandLine;

namespace RDBCli.Commands
{
    internal static class CommonCLIOptions
    {
        public static Option<string> OutputOption()
        {
            Option<string> option =
                new Option<string>(
                    aliases: new string[] { "--output", "-o" },
                    description: "The output path of parsing result.")
                .LegalFilePathsOnly();

            return option;
        }

        public static Option<string> OutputTypeOption()
        {
            Option<string> option =
                new Option<string>(
                    aliases: new string[] { "--output-type", "-ot" },
                    getDefaultValue: () => "json",
                    description: "The output type of parsing result.")
                .FromAmong("json", "html", "csv");

            return option;
        }

        public static Option<int> TopPrefixCountOption()
        {
            Option<int> option =
                new Option<int>(
                    aliases: new string[] { "--top-prefixes", "-tp" }, 
                    getDefaultValue: () => 50, 
                    description: "The number of top key prefixes.");

            option.AddValidator(x =>
            {
                var c = x.GetValueOrDefault<int>();
                if (c > 200) x.ErrorMessage = "The number can not greater than 200!!";
            });

            return option;
        }

        public static Option<int> TopBigKeyCountOption()
        {
            Option<int> option =
                new Option<int>(
                    aliases: new string[] { "--top-bigkeys", "-tb" },
                    getDefaultValue: () => 50,
                    description: "The number of top big keys.");
        
            option.AddValidator(x =>
            {
                var c = x.GetValueOrDefault<int>();
                if (c > 200) x.ErrorMessage = "The number can not greater than 200!!";
            });

            return option;
        }

        public static Option<List<int>> DBsOption()
        {
            Option<List<int>> option =
                new Option<List<int>>(
                    aliases: new string[] { "--db" },
                    description: "The filter of redis databases.");

            return option;
        }

        public static Option<List<string>> TypesOption()
        {
            Option<List<string>> option =
                new Option<List<string>>(
                    aliases: new string[] { "--type" },
                    description: "The filter of redis types.")
                .FromAmong("string", "list", "set", "sortedset", "hash", "module", "stream");

            return option;
        }

        public static Option<List<string>> KeyPrefixesOption()
        {
            Option<List<string>> option =
                new Option<List<string>>(
                    aliases: new string[] { "--key-prefix" },
                    description: "The filter of redis key prefix.");

            return option;
        }

        public static Option<bool?> KeySuffixEnableOption()
        {
            Option<bool?> option =
                new Option<bool?>(
                    aliases: new string[] { "--key-suffix-enable" },
                    description: "Use the key suffix as the key prefix.");

            return option;
        }

        public static Option<string> SeparatorsOption()
        {
            Option<string> option =
                new Option<string>(
                    aliases: new string[] { "--separators" },
                    description: "The separators of redis key prefix.");

            return option;
        }

        public static Option<int> SepPrefixCountOption()
        {
            Option<int> option =
                new Option<int>(
                    aliases: new string[] { "--sep-count" },
                    description: "The count of separating a key to prefix.");

            return option;
        }

        public static Option<bool?> IsPermanentOption()
        {
            Option<bool?> option =
                new Option<bool?>(
                    aliases: new string[] { "--permanent" },
                    description: "Whether the key is permanent.");

            return option;
        }

        public static Option<bool?> IsExpiredOption()
        {
            Option<bool?> option =
                new Option<bool?>(
                    aliases: new string[] { "--expired" },
                    description: "Whether the key is expired.");

            return option;
        }

        public static Option<bool?> IsIgnoreFieldOfLargestElemOption()
        {
            Option<bool?> option =
                new Option<bool?>(
                    aliases: new string[] { "--ignore-fole" },
                    description: "Whether ignore the field of largest elem.");

            return option;
        }

        public static Option<ulong?> MinIdleOption()
        {
            Option<ulong?> option =
                new Option<ulong?>(
                    aliases: new string[] { "--min-idle" },
                    description: "The minimum idle time of the key(must lru policy)");

            return option;
        }

        public static Option<int?> MinFreqOption()
        {
            Option<int?> option =
                new Option<int?>(
                    aliases: new string[] { "--min-freq" },
                    description: "The minimum frequency of the key(must lfu policy)");

            return option;
        }

        public static Option<string> CDNOption()
        {
            Option<string> option =
                new Option<string>(
                    aliases: new string[] { "--cdn" },
                    getDefaultValue: () => "unpkg.com",
                    description: "The cdn domain for html output");

            return option;
        }
    }
}

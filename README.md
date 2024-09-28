# rdb-tools

`rdb-tools` is a tool to parse/analysis [Redis](https://redis.io/)/[Valkey](https://valkey.io/) rdb files that is implemented by csharp.

This repository is inspired by [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools) and [rdr](https://github.com/xueqiu/rdr).

It consists of two parts:

- A parser library, that you can customize by yourself.
- An anslysis cli tool, that your can ans some basic usages for command line.

## rdb-cli

### Install

1. Download the package from the [latest stable release](https://github.com/catcherwong/rdb-tools/releases).
2. `dotnet tool install --global rdb-cli`

### Usage

Show help information:

```
[~] ./rdb-cli -h      
Description:
  rdb-cli is a command line tool, analysis redis/valkey rdb files.

Usage:
  rdb-cli [command] [options]

Options:
  -?, -h, --help  Show help and usage information
  -v, --version   Show version information

Commands:
  keys <file>    Get all keys from rdb files
  memory <file>  Get memory info from rdb files
  test <file>    Try to parser rdb files without operation
  csv <file>     Convert rdb file to csv.
```

The usage of `memory` is as follow:

```
[~] ./rdb-cli memory -h
Description:
  Analysis memory info from rdb files

Usage:
  rdb-cli memory <file> [options]

Arguments:
  <file>  The path of rdb file.

Options:
  -o, --output <output>                                  The output path of parsing result.
  -ot, --output-type <csv|html|json>                     The output type of parsing result. [default: json]
  -tp, --top-prefixes <top-prefixes>                     The number of top key prefixes. [default: 50]
  -tb, --top-bigkeys <top-bigkeys>                       The number of top big keys. [default: 50]
  --db <db>                                              The filter of redis databases.
  --type <hash|list|module|set|sortedset|stream|string>  The filter of redis types.
  --key-prefix <key-prefix>                              The filter of redis key prefix.
  --separators <separators>                              The separators of redis key prefix.
  --sep-count <sep-count>                                The count of separating a key to prefix.
  --permanent                                            Whether the key is permanent.
  --ignore-fole                                          Whether ignore the field of largest elem.
  --key-suffix-enable                                    Use the key suffix as the key prefix.
  --cdn <cdn>                                            The cdn domain for html output [default: unpkg.com]
  -?, -h, --help                                         Show help and usage information
```

```
[~] ./rdb-cli memory /tmp/demo.rdb -ot html -tb 200

Prepare to parse [/tmp/demo.rdb]
Please wait for a moment...

parse cost: 22449ms
total cost: 23107ms
result path: /tmp/res.html
```

Sample html result is as follow:

![](./static/memsample.png)

Sample json result is as follow:

```json
{
    "usedMem": 2373094496,
    "cTime": 0,
    "count": 7615333,
    "rdbVer": 9,
    "redisVer": "5.2.0",
    "redisType": "Redis",
    "redisBits": 13366,
    "typeRecords": [
        {
            "Type": "sortedset",
            "Bytes": 1385664695,
            "Num": 6212084
        }
    ],
    "largestRecords": [
        {
            "Database": 2,
            "Key": "key",
            "Bytes": 10340,
            "Type": "string",
            "Encoding": "string",
            "Expiry": 0,
            "NumOfElem": 8318,
            "LenOfLargestElem": 0,
            "FieldOfLargestElem": null,
            "Idle": 0,
            "Freq": 0
        }
     ],
     "largestKeyPrefix": [
        {
            "Type": "string",
            "Prefix": "key4",
            "Bytes": 116,
            "Num": 1,
            "Elements": 2
        }
    ],
    "expiryInfo": [
        {
            "Expiry": "0~1h",
            "Bytes": 986801692,
            "Num": 4345021
        }
    ],
    "idleOrFreqInfo": [
        {
            "Category": "0~50",
            "Bytes":1,
            "Num":1
        }
    ],
    "functions": [
        {
            "Engine": "lua",
            "LibraryName": "mylib"
        }
    ],
    "largestStreams": [
        {
            "Key": "key",
            "Length": 5,
            "LastId": "1650158935767-0",
            "FirstId": "1650158906951-0",
            "MaxDeletedEntryId": "0-0",
            "EntriesAdded": 5,
            "CGroups": 0
        }
    ],
    "dbInfo": [
        {
            "DB": "db0",
            "Bytes": 116,
            "Num": 1
        }
    ]
}
````

## RDBParse

### Install

`dotnet add package RDBParse`

### Usage

1. Implement your own `IReaderCallback`
2. Create a new instance of `BinaryReaderRDBParser`
3. Call **Parse** method of `BinaryReaderRDBParser` instance


Following this below code for example.

```cs
public class MyReaderCallBack : IReaderCallback
{
}
```

```cs
var path = "/yourpath/your.rdb"
var cb = new MyReaderCallBack();
var parser = new RDBParser.BinaryReaderRDBParser(cb);
parser.Parse(path);
```

## To contribute

Contributions are welcome!
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace RDBCli
{
    internal class RdbDataInfo
    {
        /// <summary>
        /// Do not store all records here!!!!!!!!!
        /// Producer/Consumer mode to handle the parser callback
        /// Otherwise, client's memory may increase quickly
        /// </summary>
        public BlockingCollection<AnalysisRecord> Records { get; set; } = new BlockingCollection<AnalysisRecord>(8192);

        /// <summary>
        /// Used memory, unit is byte(B)
        /// </summary>
        public long UsedMem { get; set; }

        /// <summary>
        /// Sum all callback bytes
        /// </summary>
        public ulong TotalMem { get; set; }

        /// <summary>
        /// The creation time of this RDB file, the unit is second
        /// </summary>
        public long CTime { get; set; }

        /// <summary>
        /// The count of redis key
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// The rdb version
        /// </summary>
        public int RdbVer { get; set; }

        /// <summary>
        /// The redis version from aux
        /// </summary>
        public string RedisVer { get; set; }

        /// <summary>
        /// The redis bits from aux
        /// </summary>
        public long RedisBits { get; set; }
        
        /// <summary>
        /// The information about the functions and libraries
        /// </summary>
        public List<FunctionsRecord> Functions { get; set; } = new List<FunctionsRecord>();

        /// <summary>
        /// The information about the redis type, Redis or Valkey
        /// </summary>
        public string RedisType { get; set; } = "Redis";
    }
}

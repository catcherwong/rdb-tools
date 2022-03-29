using System.Collections.Concurrent;

namespace RDBCli
{
    internal class RdbDataInfo
    {
        /// <summary>
        /// Do not store all records here!!!!!!!!!
        /// Consider of RAM in client
        /// </summary>
        public BlockingCollection<Record> Records { get; set; } = new BlockingCollection<Record>(1024);

        /// <summary>
        /// Used memory, unit is byte(B)
        /// </summary>
        public long UsedMem { get; set; }

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
    }
}

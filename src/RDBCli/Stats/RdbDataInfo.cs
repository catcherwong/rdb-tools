using System.Collections.Concurrent;

namespace RDBCli
{
    internal class RdbDataInfo
    {
        /// <summary>
        /// Do store all records here, consider of RAM
        /// </summary>
        public BlockingCollection<Record> Records { get; set; } = new BlockingCollection<Record>(1024);

        public long UsedMem { get; set; }

        public long CTime { get; set; }

        public int Count { get; set; }

        public int RdbVer { get; set; }

        public string RedisVer { get; set; }

        public long RedisBits { get; set; }
    }
}

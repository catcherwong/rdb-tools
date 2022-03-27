namespace RDBCli
{
    internal class RdbDataInfo
    {
        public System.Collections.Concurrent.ConcurrentBag<Record> Records { get; set; } = new System.Collections.Concurrent.ConcurrentBag<Record>();

        public long UsedMem { get; set; }

        public long CTime { get; set; }

        public int Count { get; set; }

        public int RdbVer { get; set; }

        public string RedisVer { get; set; }

        public long RedisBits { get; set; }
    }
}

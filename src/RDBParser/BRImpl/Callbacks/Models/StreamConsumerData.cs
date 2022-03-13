using System.Collections.Generic;

namespace RDBParser
{
    public class StreamConsumerData
    {
        public byte[] Name { get; set; }
        public ulong SeenTime { get; set; }
        public List<StreamConsumerPendingEntry> Pending { get; set; }
    }
}

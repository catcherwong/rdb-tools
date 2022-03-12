namespace RDBParser
{
    public class StreamConsumerData
    {
        public byte[] Name { get; set; }
        public ulong SeenTime { get; set; }
        public StreamConsumerPendingEntry Pending { get; set; }
    }
}

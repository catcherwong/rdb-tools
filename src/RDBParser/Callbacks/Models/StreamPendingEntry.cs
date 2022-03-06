namespace RDBParser
{
    public class StreamPendingEntry
    {
        public byte[] Id { get; set; }
        public ulong DeliveryTime { get; set; }
        public ulong DeliveryCount { get; set; }
    }
}

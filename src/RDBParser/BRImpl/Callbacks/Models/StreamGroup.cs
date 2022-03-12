namespace RDBParser
{
    public class StreamGroup
    { 
        public byte[] Name { get; set; }
        public string LastEntryId { get; set; }
        public StreamPendingEntry Pending { get; set; }
        public StreamConsumerData Consumers { get; set; }
    }
}

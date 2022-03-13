using System.Collections.Generic;

namespace RDBParser
{
    public class StreamGroup
    { 
        public byte[] Name { get; set; }
        public string LastEntryId { get; set; }
        public List<StreamPendingEntry> Pending { get; set; }
        public List<StreamConsumerData> Consumers { get; set; }
    }
}

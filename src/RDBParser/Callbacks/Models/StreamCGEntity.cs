using System.Collections.Generic;

namespace RDBParser
{
    public class StreamCGEntity
    {
        /// <summary>
        /// the consumer group name
        /// </summary>
        public byte[] Name { get; set; }
        /// <summary>
        /// Last delivered (not acknowledged) ID for this group
        /// </summary>
        public string LastEntryId { get; set; }
        /// <summary>
        /// the total number of group reads
        /// the reasoning behind this value is detailed at the top comment of streamEstimateDistanceFromFirstEverEntry
        /// </summary>
        public ulong EntriesRead { get; set; }
        /// <summary>
        /// the global PEL for this consumer group
        /// </summary>
        public List<StreamPendingEntity> Pending { get; set; }
        /// <summary>
        /// the consumers and their local PELs
        /// </summary>
        public List<StreamConsumerEntity> Consumers { get; set; }
    }
}

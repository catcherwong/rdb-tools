using System.Collections.Generic;

namespace RDBParser
{
    public class StreamConsumerEntity
    {
        /// <summary>
        /// Consumer name. This is how the consumer
        /// will be identified in the consumer group
        /// protocol. Case sensitive.
        /// </summary>
        public byte[] Name { get; set; }
        /// <summary>
        /// Last time this consumer was active.
        /// </summary>
        public ulong SeenTime { get; set; }
        /// <summary>
        /// Consumer specific pending entries list
        /// </summary>
        public List<StreamConsumerPendingEntity> Pending { get; set; }
    }
}

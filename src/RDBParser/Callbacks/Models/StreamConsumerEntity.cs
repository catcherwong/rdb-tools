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
        /// Last time this consumer tried to perform an action (attempted reading/claiming).
        /// </summary>
        public ulong SeenTime { get; set; }
        /// <summary>
        /// Last time this consumer was active (successful reading/claiming).
        /// </summary>
        public ulong ActiveTime { get; set; }
        /// <summary>
        /// Consumer specific pending entries list
        /// </summary>
        public List<StreamConsumerPendingEntity> Pending { get; set; }
    }
}

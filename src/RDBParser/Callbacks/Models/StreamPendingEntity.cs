namespace RDBParser
{
    public class StreamPendingEntity
    {
        public byte[] Id { get; set; }
        /// <summary>
        /// Last time this message was delivered.
        /// </summary>
        public ulong DeliveryTime { get; set; }
        /// <summary>
        ///  Number of times this message was delivered.
        /// </summary>
        public ulong DeliveryCount { get; set; }
    }
}

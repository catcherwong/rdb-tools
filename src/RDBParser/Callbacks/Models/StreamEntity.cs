using System.Collections.Generic;

namespace RDBParser
{
    public class StreamEntity
    {
        /// <summary>
        /// Current number of elements inside this stream
        /// </summary>
        public ulong Length { get; set; }
        /// <summary>
        /// Zero if there are yet no items.
        /// </summary>
        public string LastId { get; set; }
        /// <summary>
        /// The first non-tombstone entry, zero if empty.
        /// </summary>
        public string FirstId { get; set; }
        /// <summary>
        /// The maximal ID that was deleted.
        /// </summary>
        public string MaxDeletedEntryId { get; set; }
        /// <summary>
        /// All time count of elements added.
        /// </summary>
        public ulong EntriesAdded { get; set; }
        /// <summary>
        /// Consumer groups dictionary: name -> streamCG
        /// </summary>
        public List<StreamCGEntity> CGroups { get; set; }
    }
}

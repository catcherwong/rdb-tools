namespace RDBCli
{
    public class Record
    {
        /// <summary>
        /// The redis database that this record in
        /// </summary>
        public int Database { get; set; }

        /// <summary>
        /// The redis key of this record
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// The byte count of this record
        /// </summary>
        public ulong Bytes { get; set; }

        /// <summary>
        /// The redis type of this record, such as string, hash, etc.
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The encoding of this record, such as ziplist, linkedlist, etc.
        /// </summary>
        public string Encoding { get; set; }

        /// <summary>
        /// The expiry of this record
        /// </summary>
        public long Expiry { get; set; }

        /// <summary>
        /// The number of this record's element
        /// </summary>
        public ulong NumOfElem { get; set; }

        /// <summary>
        /// The length of this record's largest element
        /// </summary>
        public ulong LenOfLargestElem { get; set; }

        /// <summary>
        /// The field of this record's largest element
        /// </summary>
        public string FieldOfLargestElem { get; set; }

        /// <summary>
        /// LRU idle time.
        /// </summary>
        public ulong Idle { get; set; }

        /// <summary>
        /// LFU frequency.
        /// </summary>
        public int Freq { get; set; }

        /// <summary>
        /// hash slot
        /// </summary>
        public ulong SlotId { get; set; }
    }

    public class StreamsRecord
    {
        /// <summary>
        /// The redis key of this record
        /// </summary>
        public string Key { get; set; }
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
        public int CGroups { get; set; }

        public static StreamsRecord MapFromStreamsEntity(RDBParser.StreamEntity entity, byte[] key)
        {
            return new StreamsRecord
            {
                Key = System.Text.Encoding.UTF8.GetString(key),
                Length = entity.Length,
                LastId = entity.LastId,
                FirstId = entity.FirstId,
                MaxDeletedEntryId = entity.MaxDeletedEntryId,
                EntriesAdded = entity.EntriesAdded,
                CGroups = entity.CGroups.Count
            };
        }
    }

    public class AnalysisRecord
    {
        public Record Record { get; set; }
        
        public StreamsRecord StreamsRecord { get; set; }

        public AnalysisRecord(Record record)
        {
            this.Record = record;
        }
        
        public AnalysisRecord(Record record, StreamsRecord streamsRecord)
        {
            this.Record = record;
            this.StreamsRecord = streamsRecord;
        }
    }
}

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
    }
}

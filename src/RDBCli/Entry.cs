namespace RDBCli
{
    internal class Entry
    {
        public string Key { get; set; }

        public ulong Bytes { get; set; }

        public string Type { get; set; }

        public ulong NumOfElem { get; set; }

        public ulong LenOfLargestElem { get; set; }

        public string FieldOfLargestElem { get; set; }
    }
}

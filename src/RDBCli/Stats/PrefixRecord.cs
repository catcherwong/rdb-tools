using System.Collections.Generic;

namespace RDBCli
{
    public class PrefixRecord
    {
        /// <summary>
        /// The redis type, such as string hash..
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The key prefix
        /// </summary>
        public string Prefix { get; set; }

        /// <summary>
        /// The key prefix's total bytes
        /// </summary>
        public ulong Bytes { get; set; }

        /// <summary>
        /// The key prefix's total count
        /// </summary>
        public ulong Num { get; set; }

        /// <summary>
        /// The key prefix's total elements
        /// </summary>
        public ulong Elements { get; set; }

        public override string ToString()
        {
            return $"{Type}-{Prefix}-{Bytes}-{Num}-{Elements}";
        }

        public static PrefixRecordComparer Comparer = new();

        public class PrefixRecordComparer : IComparer<PrefixRecord>
        {
            public int Compare(PrefixRecord x, PrefixRecord y)
            {
                if (x.Bytes < y.Bytes)
                {
                    return -1;
                }
                else if (x.Bytes == y.Bytes)
                {
                    if (x.Num < y.Num) return -1;
                    else if (x.Num == y.Num)
                    {
                        if (x.Elements < y.Elements) return -1;
                        else if (x.Prefix.Length < y.Prefix.Length)
                            return -1;
                        else if (x.Prefix.Length == y.Prefix.Length)
                            return string.Compare(x.Prefix, y.Prefix);
                    }
                }

                return 1;
            }
        }
    }

    public class TypeRecord
    {
        /// <summary>
        /// The redis type, such as string hash..
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The redis type's total bytes
        /// </summary>
        public ulong Bytes { get; set; }

        /// <summary>
        /// The redis type's total count
        /// </summary>
        public ulong Num { get; set; }

        public override string ToString()
        {
            return $"{Type}-{Bytes}-{Num}";
        }
    }

    public class ExpiryRecord
    {
        /// <summary>
        /// The expiry category.
        /// </summary>
        public string Expiry { get; set; }

        /// <summary>
        /// The expiry category's total bytes
        /// </summary>
        public ulong Bytes { get; set; }

        /// <summary>
        /// The expiry category's total count
        /// </summary>
        public ulong Num { get; set; }

        public override string ToString()
        {
            return $"{Expiry}-{Bytes}-{Num}";
        }
    }

    public class IdleOrFreqRecord
    {
        /// <summary>
        /// The category.
        /// </summary>
        public string Category { get; set; }

        /// <summary>
        /// The expiry category's total bytes
        /// </summary>
        public ulong Bytes { get; set; }

        /// <summary>
        /// The expiry category's total count
        /// </summary>
        public ulong Num { get; set; }

        public override string ToString()
        {
            return $"{Category}-{Bytes}-{Num}";
        }
    }

    public class FunctionsRecord
    {
        public string Engine { get; set; }

        public string LibraryName { get; set; }
    }
}

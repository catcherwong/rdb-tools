using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace RDBCli
{
    internal class TypeKey
    {
        /// <summary>
        /// The redis type, such as string hash..
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The key prefix
        /// </summary>
        public string Key { get; set; }

        public override string ToString()
        {
            return $"{Type}-{Key}";
        }

        public static TypeKey FromString(string str)
        {
            var tk = str.Split('-');
            return new TypeKey { Type = tk[0], Key = tk[1] };
        }

        public static TypeKeyEqualityComparer Comparer = new TypeKeyEqualityComparer();

        public class TypeKeyEqualityComparer : IEqualityComparer<TypeKey>
        {
            public bool Equals(TypeKey x, TypeKey y)
            {
                if (WeakReference.Equals(x, y)) return true;

                if (x.Type.Equals(y.Type) && x.Key.Equals(y.Key)) return true;

                return false;
            }

            public int GetHashCode([DisallowNull] TypeKey obj)
            {
                return base.GetHashCode();
            }
        }
    }

    internal class TypeKeyValue
    {
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
    }

    internal class CommonStatValue
    {
        /// <summary>
        /// The redis type's total bytes
        /// </summary>
        public ulong Bytes { get; set; }

        /// <summary>
        /// The redis type's total count
        /// </summary>
        public ulong Num { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace RDBCli
{
    internal class TypeKey
    {
        public string Type { get; set; }
        public string Key { get; set; }

        public override string ToString()
        {
            return $"{Type}-{Key}";
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
}

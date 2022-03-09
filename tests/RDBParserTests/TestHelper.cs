using System.Collections;
using System.Collections.Generic;

namespace RDBParserTests
{
    public static class TestHelper
    {
        public static string GetRDBPath(string fileName)
        {
            var dir = System.AppContext.BaseDirectory;
            var path = System.IO.Path.Combine(dir, fileName);
            return path;
        }    
    }

    public class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        private static ByteArrayComparer _default;

        public static ByteArrayComparer Default
        {
            get
            {
                if (_default == null)
                {
                    _default = new ByteArrayComparer();
                }

                return _default;
            }
        }

        public bool Equals(byte[]? obj1, byte[]? obj2)
        {
            //    We can make use of the StructuralEqualityComparar class to see if these
            //    two arrays are equaly based on their value sequences.
            return StructuralComparisons.StructuralEqualityComparer.Equals(obj1, obj2);
        }

        public int GetHashCode(byte[] obj)
        {
            //    Just like in the Equals method, we can use the StructuralEqualityComparer
            //    class to generate a hashcode for the object.
            return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
        }
    }
}
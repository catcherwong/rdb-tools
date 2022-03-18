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

        public static byte[] GetNegativeNumberBytes(int num)
        {
            var tmp = System.BitConverter.GetBytes(num);
            var bytes = new List<byte>();
            foreach (var item in tmp)
            {
                if (item != 255)
                {
                    bytes.Add((byte)(item + 256));
                }
            }

            return bytes.ToArray();
        }

        public static bool FloatEqueal(float f1, float f2)
        {
            return System.MathF.Abs(f1 - f2) < 0.00001;
        }
    }

    public class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        private static ByteArrayComparer? _default;

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
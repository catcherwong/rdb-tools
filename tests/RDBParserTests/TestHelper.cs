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

        public static byte[] GetPosNumberBytes(long num)
        {
           
            if (num <= int.MaxValue)
            {
                byte[] src = new byte[4];
                src[0] = (byte)(((int)num >> 24) & 0xFF);
                src[1] = (byte)(((int)num >> 16) & 0xFF);
                src[2] = (byte)(((int)num >> 8) & 0xFF);
                src[3] = (byte)((int)num & 0xFF);
                System.Array.Reverse(src);
                return src;
            }
            else
            {
                byte[] tmp = System.BitConverter.GetBytes(num);
                var bytes = new List<byte>();
                foreach (var item in tmp)
                {
                    if (item != 0)
                    {
                        bytes.Add((byte)(item));
                    }
                }

                return bytes.ToArray();
            }           
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
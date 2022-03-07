using System.IO;

namespace RDBParser
{
    internal static class BinaryReaderExtenstions
    {
        public static System.UInt32 ReadUInt32BigEndian(this BinaryReader br)
        {
            var bytes = br.ReadBytes(4);
            System.Array.Reverse(bytes);
            return System.BitConverter.ToUInt32(bytes, 0);
        }

        public static System.UInt64 ReadInt64BigEndian(this BinaryReader br)
        {
            var bytes = br.ReadBytes(8);
            System.Array.Reverse(bytes);
            return System.BitConverter.ToUInt64(bytes, 0);
        }
    }
}

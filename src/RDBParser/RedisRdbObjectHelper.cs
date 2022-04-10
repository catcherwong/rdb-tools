using System;

namespace RDBParser
{
    public static class RedisRdbObjectHelper
    {
        public static bool IsInt(byte[] bytes, out long res)
        {
            var str = System.Text.Encoding.UTF8.GetString(bytes);
            if (long.TryParse(str, out res)) return true;

            var arr = str.AsSpan();
            foreach (var item in arr)
            {
                if (!char.IsNumber(item)) return false;
            }

            if (bytes.Length == 1 || bytes.Length == 2 || bytes.Length == 4)
            {
                res = ConvertBytesToInt32(bytes);
                return true;
            }

            return false;
        }

        public static int ConvertBytesToInt32(byte[] bytes)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/rdb.c#L278
            var res = 0;

            if (bytes.Length == 1)
            {
                res = (sbyte)bytes[0];
            }
            else if (bytes.Length == 2)
            {
                res = (short)(bytes[0] | (bytes[1] << 8));
            }
            else if (bytes.Length == 4)
            {
                res = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
            }

            return res;
        }

        public static byte[] ConvertInt32ToBytes(int num)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/rdb.c#L253            
            if (num >= -128 && num <= 127)
            {
                byte[] bytes = new byte[1];
                bytes[0] = (byte)(num & 0xFF);
                return bytes;
            }
            else if (num >= -32768 && num <= 32767)
            {
                byte[] bytes = new byte[2];
                bytes[0] = (byte)(num & 0xFF);
                bytes[1] = (byte)((num >> 8) & 0xFF);
                return bytes;
            }
            else if(num >= -2147483648 && num <= 2147483647)
            {
                byte[] bytes = new byte[4];
                bytes[0] = (byte)(num & 0xFF);
                bytes[1] = (byte)((num >> 8) & 0xFF);
                bytes[2] = (byte)((num >> 16) & 0xFF);
                bytes[3] = (byte)((num >> 24) & 0xFF);
                return bytes;
            }

            throw new RDBParserException("Unkown integer!!");
        }

        public static string GetStreamId(byte[] bytes)
        {
            var span = bytes.AsSpan();

            var msSpan = span.Slice(0, 8);
            var ms = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(msSpan);

            var seqSpan = span.Slice(8, 8);
            var seq = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(seqSpan);

            return $"{ms}-{seq}";
        }

        public static byte[] LpConvertInt64ToBytes(long v)
        {
            if (v >= 0 && v <= 127)
            {
                byte[] bytes = new byte[1];
                bytes[0] = (byte)v;
                return bytes;
            }
            else if (v >= -4096 && v <= 4096)
            {
                if (v < 0) v = ((long)1 << 13) + v;

                byte[] bytes = new byte[2];
                bytes[0] = (byte)((v >> 8) | 0xC0);
                bytes[1] = (byte)(v & 0xFF);
                return bytes;
            }
            else if (v >= -32768 && v <= 32768)
            {
                if (v < 0) v = ((long)1 << 16) + v;

                byte[] bytes = new byte[3];
                bytes[0] = 0xF1;
                bytes[1] = (byte)(v & 0xFF);
                bytes[2] = (byte)(v >> 8);
                return bytes;
            }
            else if (v >= -8388608 && v <= 8388607)
            {
                if (v < 0) v = ((long)1 << 24) + v;

                byte[] bytes = new byte[4];
                bytes[0] = 0xF1;
                bytes[1] = (byte)(v & 0xFF);
                bytes[2] = (byte)((v >> 8) & 0xff);
                bytes[3] = (byte)(v >> 16);
                return bytes;
            }
            else if (v >= -2147483648 && v <= 2147483647)
            {
                if (v < 0) v = ((long)1 << 32) + v;

                byte[] bytes = new byte[5];
                bytes[0] = 0xF3;
                bytes[1] = (byte)(v & 0xFF);
                bytes[2] = (byte)((v >> 8) & 0xff);
                bytes[3] = (byte)((v >> 16) & 0xff);
                bytes[4] = (byte)(v >> 24);
                return bytes;
            }
            else
            {
                byte[] bytes = new byte[9];
                bytes[0] = 0xF4;
                bytes[1] = (byte)(v & 0xFF);
                bytes[2] = (byte)((v >> 8) & 0xff);
                bytes[3] = (byte)((v >> 16) & 0xff);
                bytes[4] = (byte)((v >> 24) & 0xff);
                bytes[5] = (byte)((v >> 32) & 0xff);
                bytes[6] = (byte)((v >> 40) & 0xff);
                bytes[7] = (byte)((v >> 48) & 0xff);
                bytes[8] = (byte)(v >> 56);
                return bytes;
            }
        }

        public static long LpConvertBytesToInt64(byte[] bytes, ulong slen)
        {
            return 0;
        }
    }
}

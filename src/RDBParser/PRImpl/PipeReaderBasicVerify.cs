using System.Buffers;
using System.Text;

namespace RDBParser
{
    internal static class PipeReaderBasicVerify
    {
        private static readonly string REDIS = "REDIS";

        internal static void CheckRedisMagicString(ReadOnlySequence<byte> bytes)
        {
            var str = EncodingExtensions.GetString(Encoding.UTF8, bytes);
            if (!str.Equals(REDIS))
            {
                throw new RDBParserException("Invalid RDB File Format");
            }
        }

        internal static int CheckAndGetRDBVersion(ReadOnlySequence<byte> bytes)
        {
            var str = EncodingExtensions.GetString(Encoding.UTF8, bytes);
            if (int.TryParse(str, out var version))
            {
                if (version < Constant.RdbVersion.Min || version > Constant.RdbVersion.Max)
                {
                    throw new RDBParserException($"Invalid RDB version number {version}");
                }

                return version;
            }
            else
            {
                throw new RDBParserException($"Invalid RDB version {str}");
            }
        }
    }
}

using System.Text;

namespace RDBParser
{
    internal static class BasicVerify
    {
        private static readonly string REDIS = "REDIS";

        internal static void CheckRedisMagicString(byte[] bytes)
        {
            var str = Encoding.UTF8.GetString(bytes);
            if (!str.Equals(REDIS))
            {
                throw new RDBParserException("Invalid RDB File Format");
            }        
        }

        internal static int CheckAndGetRDBVersion(byte[] bytes)
        {
            var str = Encoding.UTF8.GetString(bytes);

            if (int.TryParse(str, out var version))
            {
                if (version < 1 || version > 9)
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

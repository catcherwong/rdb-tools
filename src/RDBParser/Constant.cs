namespace RDBParser
{
    public static class Constant
    {
        public static class RdbVersion
        { 
            public const int Min = 1;
            public const int Max = 10;
        }

        public static class MagicCount
        {
            public const int REDIS = 5;
            public const int VERSION = 4;
            public const int CHECKSUM = 8;

            public const int One = 1;
            public const int Two = 2;
            public const int Four = 4;
            public const int Eight = 8;
        }

        public static class OpCode
        {
            public const int FUNCTION2 = 245;
            public const int FUNCTION = 246;
            public const int MODULE_AUX = 247;
            public const int IDLE = 248;
            public const int FREQ = 249;
            public const int AUX = 250;
            public const int RESIZEDB = 251;
            public const int EXPIRETIME_MS = 252;
            public const int EXPIRETIME = 253;
            public const int SELECTDB = 254;
            public const int EOF = 255;
        }

        public static class LengthEncoding
        {
            public const int BIT6 = 0;
            public const int BIT14 = 1;
            public const int BIT32 = 0x80;
            public const int BIT64 = 0x81;
            public const int ENCVAL = 3;
        }

        public static class EncType
        {
            public const uint INT8 = 0;
            public const uint INT16 = 1;
            public const uint INT32 = 2;
            public const uint LZF = 3;
        }

        public static class DataType
        {
            public const int STRING = 0;
            public const int LIST = 1;
            public const int SET = 2;
            public const int ZSET = 3;
            public const int HASH = 4;

            /* >= 4.0 */
            public const int ZSET_2 = 5;
            public const int MODULE = 6;
            public const int MODULE_2 = 7;

            
            public const int HASH_ZIPMAP = 9;
            public const int LIST_ZIPLIST = 10;
            public const int SET_INTSET = 11;
            public const int ZSET_ZIPLIST = 12;
            public const int HASH_ZIPLIST = 13;

            /* >= 3.2 */
            public const int LIST_QUICKLIST = 14;

            /* >= 5.0 */
            public const int STREAM_LISTPACKS = 15;

            /* >= 7.0 */
            public const int HASH_LISTPACK = 16;
            public const int ZSET_LISTPACK = 17;
            public const int LIST_QUICKLIST_2 = 18;
            public const int STREAM_LISTPACKS_2 = 19;

            public static readonly System.Collections.Generic.Dictionary<int, string> MAPPING = new System.Collections.Generic.Dictionary<int, string>
            {
                { 0, "string" },
                { 1, "list" },
                { 2, "set" },
                { 3, "sortedset" },
                { 4, "hash" },
                { 5, "sortedset" },
                { 6, "module" },
                { 7, "module" },
                { 9, "hash" },
                { 10, "list" },
                { 11, "set" },
                { 12, "sortedset" },
                { 13, "hash" },
                { 14, "list" },
                { 15, "stream" },
                { 16, "hash" },
                { 17, "sortedset" },
                { 18, "list" },
                { 19, "stream" },
            };
        }

        public static class ModuleOpCode
        {
            public const uint EOF = 0;
            public const uint SINT = 1;
            public const uint UINT = 2;
            public const uint FLOAT = 3;
            public const uint DOUBLE = 4;
            public const uint STRING = 5;
        }

        public static class QuickListContainerFormats
        {
            public const ulong PLAIN = 1;
            public const ulong PACKED = 2;
        }

        public static class ObjEncoding
        {
            public const string STRING = "string";
            public const string HT = "hashtable";
            public const string ZIPMAP = "zipmap";
            public const string ZIPLIST = "ziplist";
            public const string INTSET = "intset";
            public const string SKIPLIST = "skiplist";
            public const string QUICKLIST = "quicklist";
            public const string LISTPACK = "listpack";
        }
    }
}
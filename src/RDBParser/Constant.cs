namespace RDBParser
{
    public static class Constant
    {
        public static class RdbVersion
        { 
            public static readonly int Min = 1;
            public static readonly int Max = 9;
        }

        public static class MagicCount
        {
            public static readonly int REDIS = 5;
            public static readonly int VERSION = 4;
            public static readonly int CHECKSUM = 8;

            public static readonly int One = 1;
            public static readonly int Two = 2;
            public static readonly int Four = 4;
            public static readonly int Eight = 8;
        }

        public static class OpCode
        {
            public static readonly int MODULE_AUX = 247;
            public static readonly int IDLE = 248;
            public static readonly int FREQ = 249;
            public static readonly int AUX = 250;
            public static readonly int RESIZEDB = 251;
            public static readonly int EXPIRETIME_MS = 252;
            public static readonly int EXPIRETIME = 253;
            public static readonly int SELECTDB = 254;
            public static readonly int EOF = 255;
        }

        public static class LengthEncoding
        {
            public static readonly int BIT6 = 0;
            public static readonly int BIT14 = 1;
            public static readonly int BIT32 = 0x80;
            public static readonly int BIT64 = 0x81;
            public static readonly int ENCVAL = 3;
        }

        public static class EncType
        {
            public static readonly uint INT8 = 0;
            public static readonly uint INT16 = 1;
            public static readonly uint INT32 = 2;
            public static readonly uint LZF = 3;
        }

        public static class DataType
        {
            public static readonly int STRING = 0;
            public static readonly int LIST = 1;
            public static readonly int SET = 2;
            public static readonly int ZSET = 3;
            public static readonly int HASH = 4;
            public static readonly int ZSET_2 = 5;
            public static readonly int MODULE = 6;
            public static readonly int MODULE_2 = 7;
            public static readonly int HASH_ZIPMAP = 9;
            public static readonly int LIST_ZIPLIST = 10;
            public static readonly int SET_INTSET = 11;
            public static readonly int ZSET_ZIPLIST = 12;
            public static readonly int HASH_ZIPLIST = 13;
            public static readonly int LIST_QUICKLIST = 14;
            public static readonly int STREAM_LISTPACKS = 15;
        }

        public static class ModuleOpCode
        {
            public static readonly uint EOF = 0;
            public static readonly uint SINT = 1;
            public static readonly uint UINT = 2;
            public static readonly uint FLOAT = 3;
            public static readonly uint DOUBLE = 4;
            public static readonly uint STRING = 5;
        }
    }
}
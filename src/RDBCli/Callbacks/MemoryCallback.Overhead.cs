using RDBParser;
using System.Collections.Generic;

namespace RDBCli.Callbacks
{
    internal partial class MemoryCallback
    {
        private ulong TopLevelObjOverhead(byte[] @string, long expiry)
        {
            // Each top level object is an entry in a dictionary, and so we have to include 
            // the overhead of a dictionary entry
            return HashtableEntryOverhead() + SizeOfString(@string) + RobjOverhead() + KeyExpiryOverhead(expiry);
        }

        private ulong ElementLength(byte[] element)
        {
            // TODO: byte[] => long
            var str = System.Text.Encoding.UTF8.GetString(element);
            if (long.TryParse(str, out _)) return 8;
            return (ulong)element.Length;
        }

        private ulong SizeOfString(byte[] @string)
        {
            var str = System.Text.Encoding.UTF8.GetString(@string);
            if (int.TryParse(str, out var num))
            {
                if (num < 10000 & num > 0) return 0;
                return 8;
            }

            ulong len = (ulong)@string.Length;

            if (len < 1 << 5)
            {
                return MemProfiler.MallocOverhead(len + 1 + 1);
            }
            else if (len < 1 << 8)
            {
                return MemProfiler.MallocOverhead(len + 1 + 2 + 1);
            }
            else if (len < 1 << 16)
            {
                return MemProfiler.MallocOverhead(len + 1 + 4 + 1);
            }
            else if (len < 1 << 32)
            {
                return MemProfiler.MallocOverhead(len + 1 + 8 + 1);
            }

            return MemProfiler.MallocOverhead(len + 1 + 16 + 1);
        }

        private ulong KeyExpiryOverhead(long expiry)
        {
            if (expiry <= 0) return 0;

            System.Threading.Interlocked.Increment(ref _dbExpires);

            // https://github.com/redis/redis/blob/6.2/src/db.c#L1418
            // https://github.com/redis/redis/blob/6.2/src/dict.c#L382
            // https://github.com/redis/redis/blob/6.2/src/dict.c#L319
            // Key expiry is stored in a hashtable, so we have to pay for the cost of a hashtable entry
            // The timestamp itself is stored as an int64, which is a 8 bytes
            return HashtableEntryOverhead() + 8;
        }

        private ulong HashtableEntryOverhead()
        {
            /*
https://github.com/redis/redis/blob/6.2/src/dict.h#L50
typedef struct dictEntry {
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictEntry *next;
} dictEntry;
             */

            // 2 pointers (*key and *next) + 8 ( v )
            return 2 * _pointerSize + 8;
        }

        private ulong HashtableOverhead(ulong size)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/dict.h#L80
            // https://github.com/redis/redis/blob/6.2.6/src/dict.h#L73
            // NOTE: Before 6.x, mainly combine dict and dictht        
            // dict = 2 pointers + 2 dictht + 1 long + 1 int
            // dictht = 1 pointer + 3 unsigned longs            
            // 4 pointers + 7 long + 1 int
            // Additionally, see **table in dictht
            // The length of the table is the next power of 2
            // When the hashtable is rehashing, another instance of **table is created
            // Due to the possibility of rehashing during loading, we calculate the worse 
            // case in which both tables are allocated, and so multiply
            // the size of **table by 1.5
            // TODO: After 7.x, some difference here.
            return 4 * _pointerSize + 7 * _longSize + 4 + NextPower(size) * _pointerSize * 3 / 2;
        }

        private ulong NextPower(ulong size)
        {
            ulong power = 1;
            while (power <= size)
            {
                power = power << 1;
            }

            return power;
        }

        private ulong LinkedlistOverhead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/adlist.h#L47
            // A list has 5 pointers + an unsigned long
            return _longSize + 5 * _pointerSize;
        }

        private ulong LinkedlistEntryOverhead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/adlist.h#L36
            // A node has 3 pointers
            return 3 * _pointerSize;
        }

        private ulong QuicklistOverhead(ulong zipCount)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/quicklist.h#L105
            // quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist
            // so 40 is ok?
            // ulong quicklist = 2 * _pointerSize + _longSize + 2 * 4;
            ulong quicklist = 40;

            // https://github.com/redis/redis/blob/6.2.6/src/quicklist.h#L124
            // 4 pointers + 1 long + 2 int
            var quickitem = 4 * _pointerSize + _longSize + 2 * 4;

            // 
            return quicklist + zipCount * quickitem;
        }

        private ulong ZiplistHeaderOverHead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/ziplist.c#L11
            // Overall Layout: <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
            // Header: <zlbytes> <zltail> <zllen> <zlend>
            // <uint32_t zlbytes> unsigned int
            // <uint32_t zltail> unsigned int
            // <uint16_t zllen>  unsigned short
            // <uint8_t zlend> unsigned char
            return 4 + 4 + 2 + 1;
        }

        private ulong ZiplistEntryOverhead(byte[] value)
        {
            ulong header = 0;
            ulong size = 0;

            var str = System.Text.Encoding.UTF8.GetString(value);
            if (long.TryParse(str, out var n))
            {
                // https://github.com/redis/redis/blob/6.2.6/src/ziplist.c#L92-L101
                // Integer encoded
                header = 1;
                if (n < 12) size = 0;
                else if (n < 256) size = 1;
                else if (n < 65536) size = 2;
                else if (n < 16777216) size = 3;
                else if (n < 4294967296) size = 4;
                else size = 8;
            }
            else
            {
                size = (ulong)value.Length;

                // https://github.com/redis/redis/blob/6.2.6/src/ziplist.c#L80-L91
                // String value
                if (size <= 63) header = 1;
                else if (size <= 16383) header = 2;
                else header = 5;
            }

            // https://github.com/redis/redis/blob/6.2.6/src/ziplist.c#L56
            // If this length is smaller than 254 bytes, it will only consume a single byte representing the length as an unsinged 8 bit integer.
            // When the length is greater than or equal to 254, it will consume 5 bytes.
            ulong prevLen = 1;
            if (size >= 254) prevLen = 5;

            // <prevlen> <encoding> <entry-data>
            return prevLen + header + size;
        }

        private ulong SkiplistOverhead(ulong size)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/server.h#L1007
            return 2 * _pointerSize + HashtableOverhead(size) + (2 * _pointerSize + 16);
        }

        private ulong SkiplistEntiryOverhead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/t_zset.c#L73
            // https://github.com/redis/redis/blob/6.2.6/src/server.h#L997
            // https://github.com/redis/redis/blob/6.2.6/src/server.h#L1001
            return HashtableEntryOverhead() + 2 * _pointerSize + 8 + (_pointerSize + 8) * ZsetRandomLevel();
        }

        private ulong RobjOverhead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/server.h#L673
            // 4 unsigned + 4 unsigned + 24 unsigned + 1 int + 1 pointer
            return 4 + 4 + 24 + 4 + _pointerSize;
        }

        private ulong ZsetRandomLevel()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/t_zset.c#L122
            ulong level = 1;
            var rint = new System.Random().Next(0, 0xFFFF);

            while (rint < 0xFFFF * 1 / 4)
            {
                level += 1;
                rint = new System.Random().Next(0, 0xFFFF);
            }

            return level < 32 ? level : 32;
        }

        private ulong SizeofStreamRadixTree(ulong numElements)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/rax.h#L133
            var numNodes = (ulong)(numElements * 2.5);
            return 16 * numElements + numNodes * 4 + numNodes * 30 * _longSize;
        }

        private ulong StreamOverhead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/stream.h#L16
            // https://github.com/redis/redis/blob/6.2.6/src/stream.h#L11
            // 2 pointers + 1 long + 1 streamID ( 2 long )
            return 2 * _pointerSize + 8 + 16;
        }

        private ulong RaxOverhead()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/rax.h#L133
            // 1 pointers + 2 long
            return _pointerSize + 2 * 8;
        }

        private ulong StreamConsumer(byte[] name)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/stream.h#L67
            // 1 mstime_t + 1 sds + 1 pointer
            return 2 * _pointerSize + 8 + SizeOfString(name);
        }

        private ulong StreamCG()
        {
            // https://github.com/redis/redis/blob/6.2.6/src/stream.h#L51
            // 1 streamID ( 2 long ) + 2 pointer
            return 2 * 8 + 2 * _pointerSize ;
        }

        private ulong StreamNACK(ulong length)
        {
            // https://github.com/redis/redis/blob/6.2.6/src/stream.h#L82
            // 1 pointer + 1 long + 1 mstime_t (1 long)
            return length * (_pointerSize + 8 + 8);
        }
    }
}

using RDBParser;
using System.Collections.Generic;

namespace RDBCli.Callbacks
{
    internal partial class MemoryCallback
    {
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
    }
}

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;

namespace RDBParser
{
    public class PipeReaderRDBParser
    {
        private readonly IReaderCallback _callback;

        public PipeReaderRDBParser(IReaderCallback callback)
        {
            this._callback = callback;
        }

        public async Task ParseAsync(string path)
        {
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                var reader = PipeReader.Create(fs);

                try
                {
                    var magicBuff = await reader.ReadBytesAsync(Constant.MagicCount.REDIS);
                    PipeReaderBasicVerify.CheckRedisMagicString(magicBuff);

                    var versionBuff = await reader.ReadBytesAsync(Constant.MagicCount.VERSION);
                    var version = PipeReaderBasicVerify.CheckAndGetRDBVersion(versionBuff);
                    _callback.StartRDB(version);

                    long db = 0;
                    long expiry = 0;
                    bool isFirstDb = true;

                    while (true)
                    {
                        ulong lruIdle = 0;
                        int lfuFreq = 0;

                        var opType = await reader.ReadSingleBytesAsync();
                        //Console.WriteLine(opType);

                        if (opType == Constant.OpCode.EXPIRETIME_MS)
                        {
                            var b = await reader.ReadBytesAsync(8);
                            expiry = 0;
                            opType = await reader.ReadSingleBytesAsync();
                        }

                        if (opType == Constant.OpCode.EXPIRETIME)
                        {
                            var b = await reader.ReadBytesAsync(4);
                            expiry = 0;
                            opType = await reader.ReadSingleBytesAsync();
                        }

                        if (opType == Constant.OpCode.IDLE)
                        {
                            var idle = await reader.ReadLengthAsync();
                            //lruIdle = idle;
                            opType = await reader.ReadSingleBytesAsync();
                        }

                        if (opType == Constant.OpCode.FREQ)
                        {
                            var freq = await reader.ReadSingleBytesAsync();
                            lfuFreq = freq;
                            opType = await reader.ReadSingleBytesAsync();
                        }

                        if (opType == Constant.OpCode.SELECTDB)
                        {
                            if (!isFirstDb)
                                _callback.EndDatabase((int)db);

                            db = await reader.ReadLengthAsync();
                            _callback.StartDatabase((int)db);
                            continue;
                        }

                        if (opType == Constant.OpCode.AUX)
                        {
                            var auxKey = await reader.ReadStringAsync();
                            var auxVal = await reader.ReadStringAsync();
                            //_callback.AuxField(auxKey, auxVal);
                            continue;
                        }

                        if (opType == Constant.OpCode.RESIZEDB)
                        {
                            var dbSize = await reader.ReadLengthAsync();
                            var expireSize = await reader.ReadLengthAsync();

                            _callback.DbSize((uint)dbSize, (uint)expireSize);
                            continue;
                        }

                        if (opType == Constant.OpCode.MODULE_AUX)
                        {
                            // ReadModule(br, null, opType, expiry, null);
                        }

                        if (opType == Constant.OpCode.EOF)
                        {
                            _callback.EndDatabase((int)db);
                            _callback.EndRDB();

                            if (version >= 5) await reader.ReadBytesAsync(Constant.MagicCount.CHECKSUM);

                            break;
                        }

                        var keyBuff = await reader.ReadStringAsync();
                        await ReadObjectAsync(reader, keyBuff, opType, expiry, null);

                        expiry = 0;
                    }

                
                }
                catch (Exception)
                {

                }
                finally
                {
                    await reader.CompleteAsync();
                }
            }
        }

        private async Task ReadObjectAsync(PipeReader reader, ReadOnlySequence<byte> key, int encType, long expiry, Info info)
        {
            if (encType == Constant.DataType.STRING)
            {
                var value = await reader.ReadStringAsync();

                info.Encoding = "string";
                //_callback.Set(key, value, expiry, info);
            }
            else if (encType == Constant.DataType.LIST)
            {
                var length = await reader.ReadLengthAsync();
                info.Encoding = "linkedlist";
                //_callback.StartList(key, expiry, info);
                while (length > 0)
                {
                    length--;
                    var val = await reader.ReadStringAsync();
                    //_callback.RPush(key, val);
                }
                //_callback.EndList(key, info);
            }
            else if (encType == Constant.DataType.SET)
            {
                var cardinality = await reader.ReadLengthAsync();
                info.Encoding = "hashtable";
                //_callback.StartSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = await reader.ReadStringAsync();
                    //_callback.SAdd(key, member);
                }
                //_callback.EndSet(key);
            }
            else if (encType == Constant.DataType.ZSET || encType == Constant.DataType.ZSET_2)
            {
                var cardinality = await reader.ReadLengthAsync();
                info.Encoding = "skiplist";
                //_callback.StartSortedSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = await reader.ReadStringAsync();

                    //double score = encType == Constant.DataType.ZSET_2
                    //    ? br.ReadDouble()
                    //    : br.ReadFloat();

                    //_callback.ZAdd(key, score, member);
                }
                //_callback.EndSortedSet(key);
            }
            else if (encType == Constant.DataType.HASH)
            {
                var length = await reader.ReadLengthAsync();

                info.Encoding = "hashtable";
                //_callback.StartHash(key, (long)length, expiry, info);

                while (length > 0)
                {
                    length--;
                    var field = await reader.ReadStringAsync();
                    var value = await reader.ReadStringAsync();

                    //_callback.HSet(key, field, value);
                }
                //_callback.EndHash(key);
            }
            else if (encType == Constant.DataType.HASH_ZIPMAP)
            {
                //ReadZipMap(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.LIST_ZIPLIST)
            {
                //ReadZipList(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.SET_INTSET)
            {
                //ReadIntSet(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.ZSET_ZIPLIST)
            {
                //ReadZSetFromZiplist(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.HASH_ZIPLIST)
            {
                //ReadHashFromZiplist(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.LIST_QUICKLIST)
            {
                //ReadListFromQuickList(br, key, expiry, info);
            }
            else if (encType == Constant.DataType.MODULE)
            {
                throw new RDBParserException($"Unable to read Redis Modules RDB objects (key {key})");
            }
            else if (encType == Constant.DataType.MODULE_2)
            {
                await reader.SkipModuleAsync();
            }
            else if (encType == Constant.DataType.STREAM_LISTPACKS)
            {
                await reader.SkipStreamAsync();
            }
            else
            {
                throw new RDBParserException($"Invalid object type {encType} for {key} ");
            }
        }
    }

    public static class PipeReaderExtensions
    {
        public static async Task<ReadOnlySequence<byte>> ReadBytesAsync(this PipeReader reader, int length)
        {
            var result = await reader.ReadAtLeastAsync(length);
            var buffer = result.Buffer;
            var data = buffer.Slice(0, length);
            reader.AdvanceTo(data.End);
            return data;
        }

        public static async Task<byte> ReadSingleBytesAsync(this PipeReader reader)
        {
            var result = await reader.ReadAtLeastAsync(1);
            var buffer = result.Buffer;
            var data = buffer.Slice(0, 1);
            var b = ReadByteItem(data);
            reader.AdvanceTo(data.End);
            return b;
        }

        public static async Task<ReadOnlySequence<byte>> ReadStringAsync(this PipeReader reader)
        {
            var (len, isEncoded) = await ReadLengthWithEncodingAsync(reader);

            if (!isEncoded) return await reader.ReadBytesAsync((int)len);

            if (len == Constant.EncType.INT8)
            {
                return await reader.ReadBytesAsync(1);
            }
            else if (len == Constant.EncType.INT16)
            {
                return await reader.ReadBytesAsync(2);
            }
            else if (len == Constant.EncType.INT32)
            {
                return await reader.ReadBytesAsync(4);
            }
            else if (len == Constant.EncType.LZF)
            {
                var clen = await reader.ReadLengthAsync();
                var ulen = await reader.ReadLengthAsync();

                var compressed = await reader.ReadBytesAsync((int)clen);
                var decompressed = LzfDecompress(compressed, (int)ulen);

                if (decompressed.Length != (int)ulen)
                    throw new RDBParserException($"decompressed string length {decompressed.Length} didn't match expected length {(int)ulen}");

                return decompressed;
            }
            else
            {
                throw new RDBParserException($"Invalid string encoding {len}");
            }
        }

        private static ReadOnlySequence<byte> LzfDecompress(ReadOnlySequence<byte> compressed, int ulen)
        {
            var outStream = new List<byte>(ulen);
            var outIndex = 0;

            Span<byte> span = null;
            compressed.CopyTo(span);

            var inLen = span.Length;
            var inIndex = 0;

            while (inIndex < inLen)
            {

                var ctrl = span[inIndex];

                inIndex = inIndex + 1;

                if (ctrl < 32)
                {
                    for (int i = 0; i < ctrl + 1; i++)
                    {
                        outStream.Add(span[inIndex]);
                        inIndex = inIndex + 1;
                        outIndex = outIndex + 1;
                    }
                }
                else
                {
                    var length = ctrl >> 5;
                    if (length == 7)
                    {
                        length = length + span[inIndex];
                        inIndex = inIndex + 1;
                    }

                    var @ref = outIndex - ((ctrl & 0x1f) << 8) - span[inIndex] - 1;
                    inIndex = inIndex + 1;

                    for (int i = 0; i < length + 2; i++)
                    {
                        outStream.Add(outStream[@ref]);
                        @ref = @ref + 1;
                        outIndex = outIndex + 1;
                    }
                }
            }

            return new ReadOnlySequence<byte>(outStream.ToArray());
        }

        public static async Task<long> ReadLengthAsync(this PipeReader reader)
        {
            var (len, _) = await ReadLengthWithEncodingAsync(reader);
            return len;
        }

        public static async Task<(long Length, bool IsEncoded)> ReadLengthWithEncodingAsync(this PipeReader reader)
        {
            long len = 0;
            var isEncoded = false;
            var b = await reader.ReadSingleBytesAsync();
            var encType = (b & 0xC0) >> 6;
            if (encType == Constant.LengthEncoding.ENCVAL)
            {
                isEncoded = true;
                len = b & 0x3F;
            }
            else if (encType == Constant.LengthEncoding.BIT6)
            {
                len = b & 0x3F;
            }
            else if (encType == Constant.LengthEncoding.BIT14)
            {
                var bb = await reader.ReadSingleBytesAsync();
                len = (b & 0x3F) << 8 | bb;
            }
            else if (b == Constant.LengthEncoding.BIT32)
            {
                var buff = await reader.ReadBytesAsync(4);
                len = buff.ReadInt32Item();
            }
            else if (b == Constant.LengthEncoding.BIT64)
            {
                var buff = await reader.ReadBytesAsync(8);
                len = buff.ReadInt64Item();
            }
            else
            {
                throw new RDBParserException($"Invalid string encoding {encType} (encoding byte {b})");
            }

            return (len, isEncoded);
        }

        public static async Task SkipStringAsync(this PipeReader reader)
        {
            long bytesToSkip = 0;

            var (len, isEncoded) = await ReadLengthWithEncodingAsync(reader);

            if (!isEncoded)
            {
                bytesToSkip = len;
            }
            else
            {
                if (len == Constant.EncType.INT8)
                {
                    bytesToSkip = 1;
                }
                else if (len == Constant.EncType.INT16)
                {
                    bytesToSkip = 2;
                }
                else if (len == Constant.EncType.INT32)
                {
                    bytesToSkip = 4;
                }
                else if (len == Constant.EncType.LZF)
                {
                    var clen = await reader.ReadLengthAsync();
                    _ = await reader.ReadLengthAsync();

                    bytesToSkip = clen;
                }
            }

            if (bytesToSkip > 0) await reader.ReadBytesAsync((int)bytesToSkip);
        }

        public static async Task SkipModuleAsync(this PipeReader reader)
        {
            _ = await reader.ReadLengthAsync();
            var opCode = await reader.ReadLengthAsync();

            while (opCode != Constant.ModuleOpCode.EOF)
            {
                if (opCode == Constant.ModuleOpCode.SINT
                    || opCode == Constant.ModuleOpCode.UINT)
                {
                    _ = await reader.ReadLengthAsync();
                }
                else if (opCode == Constant.ModuleOpCode.FLOAT)
                {
                    _ = await reader.ReadBytesAsync(4);
                }
                else if (opCode == Constant.ModuleOpCode.DOUBLE)
                {
                    _ = await reader.ReadBytesAsync(8);
                }
                else if (opCode == Constant.ModuleOpCode.STRING)
                {
                    await reader.SkipStringAsync();
                }
                else
                {
                    throw new RDBParserException($"Unknown module opcode {opCode}");
                }

                opCode = await reader.ReadLengthAsync();
            }
        }

        public static async Task SkipStreamAsync(this PipeReader reader)
        {
            var listPacks = await reader.ReadLengthAsync();

            while (listPacks > 0)
            {
                _ = await reader.ReadStringAsync();
                _ = await reader.ReadStringAsync();

                listPacks--;
            }

            _ = await reader.ReadLengthAsync();
            _ = await reader.ReadLengthAsync();
            _ = await reader.ReadLengthAsync();

            var cgroups = await reader.ReadLengthAsync();
            while (cgroups > 0)
            {
                _ = await reader.ReadStringAsync();
                _ = await reader.ReadLengthAsync();
                _ = await reader.ReadLengthAsync();
                var pending = await reader.ReadLengthAsync();
                while (pending > 0)
                {
                    _ = await reader.ReadBytesAsync(16);
                    _ = await reader.ReadBytesAsync(8);
                    _ = await reader.ReadLengthAsync();

                    pending--;
                }
                var consumers = await reader.ReadLengthAsync();
                while (consumers > 0)
                {
                    await reader.SkipStringAsync();
                    await reader.ReadBytesAsync(8);
                    pending = await reader.ReadLengthAsync();
                    await reader.ReadBytesAsync((int)(pending * 16));

                    consumers--;
                }
            }
        }

        public static byte ReadByteItem(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryRead(out var item);
            return item;
        }

        public static int ReadInt32Item(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryReadBigEndian(out int item);
            return item;
        }

        public static long ReadInt64Item(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryReadBigEndian(out long item);
            return item;
        }
    }

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

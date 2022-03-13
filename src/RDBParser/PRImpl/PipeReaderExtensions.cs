using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Tasks;

namespace RDBParser
{
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

        public static async Task<byte> ReadSingleByteAsync(this PipeReader reader)
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

                inIndex++;

                if (ctrl < 32)
                {
                    for (int i = 0; i < ctrl + 1; i++)
                    {
                        outStream.Add(span[inIndex]);
                        inIndex++;
                        outIndex++;
                    }
                }
                else
                {
                    var length = ctrl >> 5;
                    if (length == 7)
                    {
                        length += span[inIndex];
                        inIndex++;
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

        public static async Task<ulong> ReadLengthAsync(this PipeReader reader)
        {
            var (len, _) = await ReadLengthWithEncodingAsync(reader);
            return len;
        }

        public static async Task<(ulong Length, bool IsEncoded)> ReadLengthWithEncodingAsync(this PipeReader reader)
        {
            ulong len = 0;
            var isEncoded = false;
            var b = await reader.ReadSingleByteAsync();
            var encType = (b & 0xC0) >> 6;
            if (encType == Constant.LengthEncoding.ENCVAL)
            {
                isEncoded = true;
                len = (ulong)b & 0x3F;
            }
            else if (encType == Constant.LengthEncoding.BIT6)
            {
                len = (ulong)b & 0x3F;
            }
            else if (encType == Constant.LengthEncoding.BIT14)
            {
                var bb = await reader.ReadSingleByteAsync();
                len = (ulong)(b & 0x3F) << 8 | bb;
            }
            else if (b == Constant.LengthEncoding.BIT32)
            {
                var buff = await reader.ReadBytesAsync(4);
                len = (ulong)buff.ReadInt32BigEndianItem();
            }
            else if (b == Constant.LengthEncoding.BIT64)
            {
                var buff = await reader.ReadBytesAsync(8);
                len = (ulong)buff.ReadInt64BigEndianItem();
            }
            else
            {
                throw new RDBParserException($"Invalid string encoding {encType} (encoding byte {b})");
            }

            return (len, isEncoded);
        }

        public static async Task SkipStringAsync(this PipeReader reader)
        {
            ulong bytesToSkip = 0;

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
        
        public static async Task<double> ReadDoubleAsync(this PipeReader reader)
        {
            var buff = await reader.ReadBytesAsync(8);
            return BinaryPrimitives.ReadDoubleLittleEndian(buff.FirstSpan);
        }

        public static async Task<float> ReadFloatAsync(this PipeReader reader)
        {
            var len = await reader.ReadSingleByteAsync();

            if (len == 253) return float.NaN;
            else if (len == 254) return float.PositiveInfinity;
            else if (len == 255) return float.NegativeInfinity;

            var data = await reader.ReadBytesAsync(len);
            var str = EncodingExtensions.GetString(Encoding.UTF8, data);
            return float.TryParse(str, out var res)
                ? res
                : 0;
        }

        public static byte ReadByteItem(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryRead(out var item);
            return item;
        }

        public static int ReadInt32BigEndianItem(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryReadBigEndian(out int item);
            return item;
        }

        public static long ReadInt64BigEndianItem(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryReadBigEndian(out long item);
            return item;
        }

        public static int ReadInt32LittleEndianItem(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryReadLittleEndian(out int item);
            return item;
        }

        public static long ReadInt64LittleEndianItem(this ReadOnlySequence<byte> buff)
        {
            SequenceReader<byte> reader = new(buff);
            reader.TryReadLittleEndian(out long item);
            return item;
        }
    }
}

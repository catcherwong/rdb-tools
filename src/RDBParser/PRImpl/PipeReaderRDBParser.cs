using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace RDBParser
{
    public partial class PipeReaderRDBParser : IRDBParser
    {
        private readonly IPipeReaderCallback _callback;
        private ReadOnlySequence<byte> _key;
        private long _expiry = 0;
        private ulong _idle = 0;
        private int _freq = 0;

        public PipeReaderRDBParser(IPipeReaderCallback callback)
        {
            this._callback = callback;
        }

        public void Parse(string path)
            => ParseAsync(path).GetAwaiter().GetResult();

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

                    int db = 0;
                    bool isFirstDb = true;

                    while (true)
                    {
                        var opType = await reader.ReadSingleByteAsync();

                        if (opType == Constant.OpCode.EXPIRETIME_MS)
                        {
                            var b = await reader.ReadBytesAsync(8);
                            _expiry = b.ReadInt64LittleEndianItem();
                            opType = await reader.ReadSingleByteAsync();
                        }

                        if (opType == Constant.OpCode.EXPIRETIME)
                        {
                            var b = await reader.ReadBytesAsync(4);
                            _expiry = b.ReadInt32LittleEndianItem();
                            opType = await reader.ReadSingleByteAsync();
                        }

                        if (opType == Constant.OpCode.IDLE)
                        {
                            var idle = await reader.ReadLengthAsync();
                            _idle = idle;
                            opType = await reader.ReadSingleByteAsync();
                        }

                        if (opType == Constant.OpCode.FREQ)
                        {
                            var freq = await reader.ReadSingleByteAsync();
                            _freq = freq;
                            opType = await reader.ReadSingleByteAsync();
                        }

                        if (opType == Constant.OpCode.SELECTDB)
                        {
                            if (!isFirstDb)
                                _callback.EndDatabase(db);

                            isFirstDb = false;
                            var len = await reader.ReadLengthAsync();
                            db = ((int)len);
                            _callback.StartDatabase(db);
                            continue;
                        }

                        if (opType == Constant.OpCode.AUX)
                        {
                            var auxKey = await reader.ReadStringAsync();
                            var auxVal = await reader.ReadStringAsync();
                            _callback.AuxField(auxKey, auxVal);
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
                            await reader.SkipModuleAsync();
                        }

                        if (opType == Constant.OpCode.EOF)
                        {
                            _callback.EndDatabase((int)db);
                            _callback.EndRDB();

                            if (version >= 5) await reader.ReadBytesAsync(Constant.MagicCount.CHECKSUM);

                            break;
                        }

                        _key = await reader.ReadStringAsync();
                        await ReadObjectAsync(reader, _key, opType, _expiry, null);

                        _expiry = 0;
                    }
                }
                catch (Exception ex)
                {
                    System.Console.WriteLine(ex.Message);
                    System.Console.WriteLine(ex.StackTrace);
                }
                finally
                {
                    await reader.CompleteAsync();
                }
            }
        }
    }
}

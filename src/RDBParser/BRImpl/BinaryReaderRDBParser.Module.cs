using System.Collections.Generic;
using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadModule(BinaryReader br, byte[] key, int encType, long expiry, Info info)
        {
            var wrapper = new IOWrapper(br.BaseStream);
            wrapper.StartRecordingSize();
            wrapper.StartRecording();
            var length = wrapper.ReadLength();
            var isRecordBuffer = _callback.StartModule(key, DecodeModuleId(length), expiry, info);

            if (!isRecordBuffer) wrapper.StopRecording();

            var opCode = wrapper.ReadLength();

            while (opCode != Constant.ModuleOpCode.EOF)
            {
                byte[] data;

                if (opCode == Constant.ModuleOpCode.SINT
                    || opCode == Constant.ModuleOpCode.UINT)
                {
                    data = new byte[] { (byte)wrapper.ReadLength() };
                }
                else if (opCode == Constant.ModuleOpCode.FLOAT)
                {
                    data = wrapper.ReadBytes(4);
                }
                else if (opCode == Constant.ModuleOpCode.DOUBLE)
                {
                    data = wrapper.ReadBytes(8);
                }
                else if (opCode == Constant.ModuleOpCode.STRING)
                {
                    data = wrapper.ReadStr();
                }
                else
                {
                    throw new RDBParserException($"Unknown module opcode {opCode}");
                }

                _callback.HandleModuleData(key, opCode, data);

                opCode = wrapper.ReadLength();
            }

            byte[] buff = null;

            if (isRecordBuffer)
            {
                var tmp = new List<byte>();
                tmp.Add(0x07);
                tmp.AddRange(wrapper.GetRecordedBuff());
                buff = tmp.ToArray();
                wrapper.StopRecording();
            }

            _callback.EndModule(key, wrapper.GetRecordedSize(), buff);
        }
        
        private static string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        
        private string DecodeModuleId(ulong moduleId)
        {
            int len = 9;

            var name = new string[len];
            moduleId >>= 10;

            while (len > 0)
            {
                len--;
                var idx = moduleId & 63;
                name[len] = charset[(int)idx].ToString();
                moduleId >>= 6;
            }

            return string.Join("", name);
        }
        
        public class IOWrapper : BinaryReader
        {
            private bool _recordBuff;
            private bool _recordBuffSize;
            private List<byte> _bytes;
            private long _buffSize;

            public IOWrapper(Stream input) : base(input)
            {
                _bytes = new List<byte>();
            }

            public void StartRecording()
                => _recordBuff = true;

            public void StartRecordingSize()
                => _recordBuffSize = true;

            public byte[] GetRecordedBuff()
                => _bytes.ToArray();

            public long GetRecordedSize()
                => _buffSize;

            public void StopRecording()
            {
                _recordBuff = false;
                _bytes = new List<byte>();
            }

            public void StopRecordingSize()
            {
                _recordBuffSize = false;
                _buffSize = 0;
            }

            public override byte ReadByte()
            {
                var b = base.ReadByte();

                if (_recordBuff) _bytes.Add(b);

                if (_recordBuffSize) _buffSize += 1;

                return b;
            }

            public override byte[] ReadBytes(int count)
            {
                var bytes = base.ReadBytes(count);

                if (_recordBuff) _bytes.AddRange(bytes);

                if (_recordBuffSize) _buffSize += bytes.Length;

                return bytes;
            }
        }
    }
}

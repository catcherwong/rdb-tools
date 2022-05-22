using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser : IRDBParser
    {
        private void ReadListFromQuickList(BinaryReader br, int encType)
        {
            var length = br.ReadLength();
            var totalSize = 0;
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = Constant.ObjEncoding.QUICKLIST;
            info.Zips = length;
            _callback.StartList(_key, _expiry, info);

            while (length > 0)
            {
                length--;

                if (encType == Constant.DataType.LIST_QUICKLIST_2)
                {
                    var container = br.ReadLength();

                    if (container != Constant.QuickListContainerFormats.PACKED
                        && container != Constant.QuickListContainerFormats.PLAIN)
                    {
                        throw new RDBParserException("Quicklist integrity check failed.");
                    }
                }

                var rawString = br.ReadStr();
                totalSize += rawString.Length;

                using (MemoryStream stream = new MemoryStream(rawString))
                {
                    var rd = new BinaryReader(stream);

                    if (encType == Constant.DataType.LIST_QUICKLIST_2)
                    {
                        // https://github.com/redis/redis/blob/7.0-rc3/src/listpack.c#L1284
                        // <total_bytes>
                        var bytes = lpGetTotalBytes(rd);
                        // <size>
                        var numEle = lpGetNumElements(rd);

                        info.Encoding = Constant.ObjEncoding.LISTPACK;

                        for (int i = 0; i < numEle; i++)
                        {
                            // <entry>
                            var entry = ReadListPackEntry(rd);
                            _callback.RPush(_key, entry.data);
                        }

                        var lpEnd = rd.ReadByte();
                        if (lpEnd != 0xFF) throw new RDBParserException($"Invalid list pack end - {lpEnd} for key {_key}");
                    }
                    else
                    {
                        var zlbytes = rd.ReadBytes(4);
                        var tailOffset = rd.ReadBytes(4);
                        var numEntries = rd.ReadUInt16();

                        for (int i = 0; i < numEntries; i++)
                        {
                            _callback.RPush(_key, ReadZipListEntry(rd));
                        }

                        var zlistEnd = rd.ReadByte();
                        if (zlistEnd != 255)
                        {
                            throw new RDBParserException("Invalid zip list end");
                        }
                    }
                }
            }

            info.SizeOfValue = totalSize;

            _callback.EndList(_key, info);
        }

        private void SkipListFromQuickList(BinaryReader br, int encType)
        {
            var length = br.ReadLength();
            while (length > 0)
            {
                length--;
                if (encType == Constant.DataType.LIST_QUICKLIST_2)
                {
                    br.ReadLength();
                }

                br.SkipStr();
            }
        }
    }
}

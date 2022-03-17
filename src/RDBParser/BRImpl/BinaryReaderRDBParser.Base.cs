using System.IO;

namespace RDBParser
{
    public partial class BinaryReaderRDBParser
    {
        public void ReadObject(BinaryReader br, byte[] key, int encType, long expiry, Info info)
        {
            if (encType == Constant.DataType.STRING)
            {
                var value = br.ReadStr();

                info.Encoding = "string";
                _callback.Set(key, value, expiry, info);
            }
            else if (encType == Constant.DataType.LIST)
            {
                var length = br.ReadLength();
                info.Encoding = "linkedlist";
                _callback.StartList(key, expiry, info);
                while (length > 0)
                {
                    length--;
                    var val = br.ReadStr();
                    _callback.RPush(key, val);
                }
                _callback.EndList(key, info);
            }
            else if (encType == Constant.DataType.SET)
            {
                var cardinality = br.ReadLength();
                info.Encoding = "hashtable";
                _callback.StartSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = br.ReadStr();
                    _callback.SAdd(key, member);
                }
                _callback.EndSet(key);
            }
            else if (encType == Constant.DataType.ZSET || encType == Constant.DataType.ZSET_2)
            {
                var cardinality = br.ReadLength();
                info.Encoding = "skiplist";
                _callback.StartSortedSet(key, (long)cardinality, expiry, info);
                while (cardinality > 0)
                {
                    cardinality--;
                    var member = br.ReadStr();

                    double score = encType == Constant.DataType.ZSET_2
                        ? br.ReadDouble()
                        : br.ReadFloat();

                    _callback.ZAdd(key, score, member);
                }
                _callback.EndSortedSet(key);
            }
            else if (encType == Constant.DataType.HASH)
            {
                var length = br.ReadLength();

                info.Encoding = "hashtable";
                _callback.StartHash(key, (long)length, expiry, info);

                while (length > 0)
                {
                    length--;
                    var field = br.ReadStr();
                    var value = br.ReadStr();

                    _callback.HSet(key, field, value);
                }
                _callback.EndHash(key);
            }
            else if (encType == Constant.DataType.HASH_ZIPMAP)
            {
                ReadZipMap(br);
            }
            else if (encType == Constant.DataType.LIST_ZIPLIST)
            {
                ReadZipList(br);
            }
            else if (encType == Constant.DataType.SET_INTSET)
            {
                ReadIntSet(br);
            }
            else if (encType == Constant.DataType.ZSET_ZIPLIST)
            {
                ReadZSetFromZiplist(br);
            }
            else if (encType == Constant.DataType.HASH_ZIPLIST)
            {
                ReadHashFromZiplist(br);
            }
            else if (encType == Constant.DataType.LIST_QUICKLIST)
            {
                ReadListFromQuickList(br);
            }
            else if (encType == Constant.DataType.MODULE)
            {
                throw new RDBParserException($"Unable to read Redis Modules RDB objects (key {key})");
            }
            else if (encType == Constant.DataType.MODULE_2)
            {
                ReadModule(br);
            }
            else if (encType == Constant.DataType.STREAM_LISTPACKS)
            {
                ReadStream(br);
            }
            else
            {
                throw new RDBParserException($"Invalid object type {encType} for {key} ");
            }
        }

        private void ReadListFromQuickList(BinaryReader br)
        {
            var length = br.ReadLength();
            var totalSize = 0;
            Info info = new Info();
            info.Idle = _idle;
            info.Freq = _freq;
            info.Encoding = "quicklist";
            info.Zips = length;
            _callback.StartList(_key, _expiry, info);

            while (length > 0)
            {
                length--;

                var rawString = br.ReadStr();
                totalSize += rawString.Length;

                using (MemoryStream stream = new MemoryStream(rawString))
                {
                    var rd = new BinaryReader(stream);
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

            _callback.EndList(_key, info);
        }
    }
}

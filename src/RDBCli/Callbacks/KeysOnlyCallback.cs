using RDBParser;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;

namespace RDBCli.Callbacks
{
    internal class KeysOnlyCallback : RDBParser.IReaderCallback
    {
        private IConsole _console;
        private List<string> _prefixes;
        private bool? _isPermanent;

        public KeysOnlyCallback(IConsole console, List<string> prefixes, bool? isPermanent)
        {
            this._console = console;
            this._prefixes = prefixes;
            this._isPermanent = isPermanent;
        }

        private void OutputInfo(byte[] key, long expiry)
        {
            var keyStr = System.Text.Encoding.UTF8.GetString(key);

            if(CheckPreifx(keyStr))
            {
                if(_isPermanent.HasValue)
                {
                    if(_isPermanent.Value && expiry == 0)
                    {
                        _console.WriteLine(keyStr);
                    }
                    else if(!_isPermanent.Value && expiry != 0)
                    {
                        _console.WriteLine(keyStr);
                    }
                }
                else
                {
                    _console.WriteLine(keyStr);
                }
            }
        }

        private bool CheckPreifx(string key)
        {
            var flag = false;

            if(_prefixes != null && _prefixes.Any())
            {
                foreach(var item in _prefixes)
                {
                    if(key.StartsWith(item))
                    {
                        flag = true;
                        break;
                    }
                }
            }
            else
            {
                flag = true;
            }

            return flag;
        }

        public void AuxField(byte[] key, byte[] value)
        {
        }

        public void DbSize(uint dbSize, uint expiresSize)
        {
        }

        public void EndDatabase(int dbNumber)
        {
        }

        public void EndHash(byte[] key)
        {
        }

        public void EndList(byte[] key, Info info)
        {
        }

        public void EndModule(byte[] key, long bufferSize, byte[] buffer)
        {
        }

        public void EndRDB()
        {
        }

        public void EndSet(byte[] key)
        {
        }

        public void EndSortedSet(byte[] key)
        {
        }

        public void EndStream(byte[] key, StreamEntity entity)
        {
        }

        public void FuntionLoad(byte[] engine, byte[] libName, byte[] code)
        {
        }

        public void HandleModuleData(byte[] key, ulong opCode, byte[] data)
        {
        }

        public void HSet(byte[] key, byte[] field, byte[] value)
        {
        }

        public void RPush(byte[] key, byte[] value)
        {
        }

        public void SAdd(byte[] key, byte[] member)
        {
        }

        public void Set(byte[] key, byte[] value, long expiry, Info info)
        {
            OutputInfo(key, expiry);
        }

        public void StartDatabase(int database)
        {
        }

        public void StartHash(byte[] key, long length, long expiry, Info info)
        {
            OutputInfo(key, expiry);
        }

        public void StartList(byte[] key, long expiry, Info info)
        {
            OutputInfo(key, expiry);
        }

        public bool StartModule(byte[] key, string module_name, long expiry, Info info)
        {
            if (key != null && key.Length > 0)
                OutputInfo(key, expiry);

            return false;
        }

        public void StartRDB(int version)
        {
        }

        public void StartSet(byte[] key, long cardinality, long expiry, Info info)
        {
            OutputInfo(key, expiry);
        }

        public void StartSortedSet(byte[] key, long length, long expiry, Info info)
        {
            OutputInfo(key, expiry);
        }

        public void StartStream(byte[] key, long listpacks_count, long expiry, Info info)
        {
            OutputInfo(key, expiry);
        }

        public void StreamListPack(byte[] key, byte[] entry_id, byte[] data)
        {
        }

        public void ZAdd(byte[] key, double score, byte[] member)
        {
        }
    }
}

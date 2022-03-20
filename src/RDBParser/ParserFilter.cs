using System.Collections.Generic;

namespace RDBParser
{
    public class ParserFilter
    {
        public List<int> Databases { get; set; }

        public List<string> Types { get; set; }

        public List<byte[]> Keys { get; set; }
    }
}

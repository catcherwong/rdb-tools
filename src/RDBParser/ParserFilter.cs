using System.Collections.Generic;

namespace RDBParser
{
    public class ParserFilter
    {
        /// <summary>
        /// The databases that you need
        /// </summary>
        public List<int> Databases { get; set; }

        /// <summary>
        /// The types that you need
        /// </summary>
        public List<string> Types { get; set; }
    }
}

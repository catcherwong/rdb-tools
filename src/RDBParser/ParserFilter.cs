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

        /// <summary>
        /// The key prefixes that you need
        /// </summary>
        public List<string> KeyPrefixes { get; set; }

        /// <summary>
        /// Whether the key's expiry is permanent or not
        /// </summary>
        public bool? IsPermanent { get; set; }
    }
}

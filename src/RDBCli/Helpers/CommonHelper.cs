namespace RDBCli
{
    internal static class CommonHelper
    {
        /// <summary>
        /// Get a fuzzy redis version from a rdb version.
        /// Mainly for not get `redis-ver` from aux field.
        /// </summary>
        /// <param name="rdbVer">RDB VERSION </param>
        /// <returns>REDIS VERSION</returns>
        internal static string GetFuzzyRedisVersion(int rdbVer)
        {
            var ver = "unknow";

            if (rdbVer == 10)
            {
                ver = "7.x";
            }
            else if (rdbVer == 9)
            {
                // 5.0.0 ~ 5.0.14 ~ 6.2.6
                ver = "5.x";
            }
            else if (rdbVer == 8)
            {
                // 4.0.0 ~ 4.0.14
                ver = "4.0.x";
            }
            else if (rdbVer == 7)
            {
                // 3.2.0 ~ 3.2.13
                ver = "3.2.x";
            }
            else if (rdbVer == 6)
            {
                // 2.6.0 ~ 2.8.24 ~ 3.0.7
                ver = "2.8.x";
            }
            else if (rdbVer <= 5)
            {
                ver = "2.x";
            }
           
            return ver;
        }
    }
}

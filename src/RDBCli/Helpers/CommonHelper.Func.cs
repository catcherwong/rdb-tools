using System;

namespace RDBCli
{
    internal static partial class CommonHelper
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

            if (rdbVer == 11)
            {
                ver = "7.2.x";
            }
            else if (rdbVer == 10)
            {
                ver = "7.0.x";
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

        internal static string GetExpireString(long exp)
        {
            var res = exp.ToString();

            if (exp > 0)
            {
                var sub = DateTimeOffset.FromUnixTimeMilliseconds(exp).Subtract(DateTimeOffset.UtcNow);

                // 0~1h, 1~3h, 3~12h, 12~24h, 24~72h, 72~168h, 168h~
                var hour = sub.TotalHours;
                if (hour <= 0)
                {
                    res = AlreadyExpired;
                }
                else if (hour > 0 && hour < 1)
                {
                    res = "0~1h";
                }
                else if (hour >= 1 && hour < 3)
                {
                    res = "1~3h";
                }
                else if (hour >= 3 && hour < 12)
                {
                    res = "3~12h";
                }
                else if (hour >= 12 && hour < 24)
                {
                    res = "12~24h";
                }
                else if (hour >= 24 && hour < 72)
                {
                    res = "1~3d";
                }
                else if (hour >= 72 && hour < 168)
                {
                    res = "3~7d";
                }
                else if (hour >= 168)
                {
                    res = ">7d";
                }
            }
            else if (exp == 0)
            {
                res = Permanent;
            }

            return res;
        }

        internal static string GetIdleString(ulong idle)
        {
            var res = idle.ToString();

            // 0~1h = 3600, 1~3h = 10800, 3~12h = 43200, 12~24h = 86400, 1~3d = 259200, 3~7d = 604800

            if (idle < 3600)
            {
                res = "0~1h";
            }
            else if (idle >= 3600 && idle < 10800)
            {
                res = "1~3h";
            }
            else if (idle >= 10800 && idle < 43200)
            {
                res = "3~12h";
            }
            else if (idle >= 43200 && idle < 86400)
            {
                res = "12~24h";
            }
            else if (idle >= 86400 && idle < 259200)
            {
                res = "1~3d";
            }
            else if (idle >= 259200 && idle < 604800)
            {
                res = "3~7d";
            }
            else if (idle >= 604800)
            {
                res = ">7d";
            }

            return res;
        }

        internal static string GetFreqString(int freq)
        {
            // 0~50, 50~100, 100~150, 150~200, 200~250, 250~
            var res = freq.ToString();

            if (freq < 50)
            {
                res = "0~50";
            }
            else if (freq >= 50 && freq < 100)
            {
                res = "50~100";
            }
            else if (freq >= 100 && freq < 150)
            {
                res = "100~150";
            }
            else if (freq >= 150 && freq < 200)
            {
                res = "150~200";
            }
            else if (freq >= 200 && freq < 250)
            {
                res = "200~250";
            }
            else if (freq >= 250)
            {
                res = ">250";
            }

            return res;

        }

        internal const char SplitChar = ':';
        internal static string GetShortKey(string key)
        {
            var len = key.Length;

            if (len > 1024)
            {
                var span = key.AsSpan();

                var b = span.Slice(0, 10).ToString();
                var e = span.Slice(len - 6, 5).ToString();

                var n = $"{b}...({len - 15} more bytes)...{e}";
                return n;
            }

            return key;
        }
    }
}

using System;

namespace RDBCli
{
    internal static class CommonHelper
    {
        // compress Tpl/tpl.html to get this value.
        internal const string TplHtmlString = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta http-equiv=\"X-UA-Compatible\"content=\"IE=edge\"><meta name=\"viewport\"content=\"width=device-width, initial-scale=1.0\"><title>rdb-cli,offline key analysis</title><link rel=\"stylesheet\"href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css\"/><link rel=\"stylesheet\"href=\"https://cdn.jsdelivr.net/npm/bootstrap-table@1.19.1/dist/bootstrap-table.min.css\"><style>.bootstrap-table .fixed-table-pagination > .pagination-detail .page-list{display:none}</style><script src=\"https://cdn.jsdelivr.net/npm/jquery@3.6.0/dist/jquery.min.js\"></script><script src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.min.js\"></script><script src=\"https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js\"></script><script src=\"https://cdn.jsdelivr.net/npm/bootstrap-table@1.19.1/dist/bootstrap-table.min.js\"></script></head><body><nav class=\"navbar navbar-expand-sm bg-info navbar-dark\"><div class=\"ms-auto me-auto\"><a class=\"navbar-brand fs-4\"href=\"https://github.com/catcherwong/rdb-tools\">rdb-cli</a></div></nav><div class=\"container\"><br/><div class=\"page-header border-start border-light border-3\"><h3>Basic Information</h3></div><br/><div class=\"row\"><div class=\"col-md-4\"><div class=\"card\"style=\"height: 100px;\"><div class=\"card-body text-center\"><div class=\"lead display-7\">RDB Version:<b id=\"rdbVer\"></b></div><div class=\"lead display-7\">Redis Version:<b id=\"redisVer\"></b></div></div></div></div><div class=\"col-md-4\"><div class=\"card\"style=\"height: 100px;\"><div class=\"card-body text-center\"><div id=\"totalKeyBytes\"class=\"lead text-primary display-6\"></div><p class=\"small text-muted\">Total Memory Usage of Keys</p></div></div></div><div class=\"col-md-4\"><div class=\"card\"style=\"height: 100px;\"><div class=\"card-body text-center\"><div id=\"totalKeyNum\"class=\"lead text-primary display-6\"></div><p class=\"small text-muted\">Total Number of Keys</p></div></div></div></div><br/><div class=\"page-header border-start border-light border-3\"><h3>Details Information</h3></div><br/><div class=\"row\"><div class=\"col-md-6\"><canvas id=\"typeNum\"width=\"400\"height=\"400\"></canvas></div><div class=\"col-md-6\"><canvas id=\"typeByte\"width=\"400\"height=\"400\"></canvas></div></div><br/><br/><div class=\"row\"><div class=\"col-md-6\"><canvas id=\"expiryNum\"width=\"400\"height=\"400\"></canvas></div><div class=\"col-md-6\"><canvas id=\"expiryByte\"width=\"400\"height=\"400\"></canvas></div></div><br/><br/><div class=\"row\"><div class=\"col-md-12\"><ul class=\"nav nav-tabs\"role=\"tablist\"><li class=\"nav-item\"><a class=\"nav-link active\"data-bs-toggle=\"tab\"href=\"#topPrefix\">Top Key Prefixes</a></li><li class=\"nav-item\"><a class=\"nav-link\"data-bs-toggle=\"tab\"href=\"#topBigKeys\">Top Big Keys</a></li><li class=\"nav-item\"id=\"tbStreams\"><a class=\"nav-link\"data-bs-toggle=\"tab\"href=\"#streams\">Top Streams</a></li><li class=\"nav-item\"id=\"tbFunctions\"><a class=\"nav-link\"data-bs-toggle=\"tab\"href=\"#functions\">Functions</a></li></ul><div class=\"tab-content\"><div id=\"topBigKeys\"class=\"container tab-pane fade\"><br><table id=\"topBigKeysTable\"></table></div><div id=\"topPrefix\"class=\"container tab-pane active\"><br><table id=\"topPrefixTable\"></table></div><div id=\"functions\"class=\"container tab-pane fade\"><br><table id=\"functionTable\"></table></div><div id=\"streams\"class=\"container tab-pane fade\"><br><table id=\"streamsTable\"></table></div></div></div></div></div>" +
            "<script>const cliData={{CLIDATA}};$('#rdbVer').text(cliData.rdbVer);$('#redisVer').text(cliData.redisVer);$('#totalKeyBytes').text(formatBytes(cliData.usedMem));$('#totalKeyNum').text(formatNumber(cliData.count));buildTypeNumChart();buildTypeByteChart();buildExpiryNumChart();buildExpiryByteChart();$('#topPrefixTable').bootstrapTable({data:cliData.largestKeyPrefix,columns:[{field:'Prefix',title:'Key Prefix'},{field:'Type',title:'Type'},{field:'Bytes',title:'Memory Usage',sortable:true,formatter:function(val,row,index){return formatBytes(val)}},{field:'Num',title:'Keys',sortable:true,formatter:function(val,row,index){return formatNumber(val)}},{field:'Elements',title:'Elements',sortable:true,formatter:function(val,row,index){return formatNumber(val)}}],pagination:true,pageSize:10,pageNumber:1,});$('#topBigKeysTable').bootstrapTable({data:cliData.largestRecords,columns:[{field:'Key',title:'Key'},{field:'Type',title:'Type'},{field:'Encoding',title:'Encoding'},{field:'Bytes',title:'Memory Usage',sortable:true,formatter:function(val,row,index){return formatBytes(val)}},{field:'Database',title:'Database'},{field:'Expiry',title:'Validity Period',formatter:function(val,row,index){return formatExpiry(val)}},{field:'NumOfElem',title:'Elements',sortable:true,formatter:function(val,row,index){return formatNumber(val)}},{field:'LenOfLargestElem',title:'Length of Largest Element',sortable:true,}],pagination:true,pageSize:10,pageNumber:1,});$('#functionTable').bootstrapTable({data:cliData.functions,columns:[{field:'Engine',title:'Engine'},{field:'LibraryName',title:'Library Name'}],pagination:true,pageSize:10,pageNumber:1,});$('#streamsTable').bootstrapTable({data:cliData.largestStreams,columns:[{field:'Key',title:'Key'},{field:'Length',title:'Length',sortable:true,},{field:'LastId',title:'LastId'},{field:'FirstId',title:'FirstId'},{field:'MaxDeletedEntryId',title:'Max Deleted EntryId'},{field:'EntriesAdded',title:'Entries Added',sortable:true,},{field:'CGroups',title:'Consumer Groups',sortable:true,}],pagination:true,pageSize:10,pageNumber:1,});function buildTypeNumChart(){const typeChartLabels=cliData.typeRecords.map(item=>item.Type);const typeNumData=cliData.typeRecords.map(item=>item.Num);buildBarChart(typeChartLabels,typeNumData,'Keys','Distribution of Keys',formatNumber,'typeNum')};function buildTypeByteChart(){const typeChartLabels=cliData.typeRecords.map(item=>item.Type);const typeByteData=cliData.typeRecords.map(item=>item.Bytes);buildBarChart(typeChartLabels,typeByteData,'Memory Usage of Keys','Memory Usage of Keys',formatBytes,'typeByte')};function buildExpiryNumChart(){const expiryLabels=cliData.expiryInfo.map(item=>item.Expiry);const expiryData=cliData.expiryInfo.map(item=>item.Num);buildBarChart(expiryLabels,expiryData,'Total Keys','Distribution of Key Expiration Time (Quantity)',formatNumber,'expiryNum')};function buildExpiryByteChart(){const expiryLabels=cliData.expiryInfo.map(item=>item.Expiry);const expiryData=cliData.expiryInfo.map(item=>item.Bytes);buildBarChart(expiryLabels,expiryData,'Memory Usage of Keys','Distribution of Key Expiration Time (Memory)',formatBytes,'expiryByte')};function buildBarChart(labels,dsData,dsLabel,title,formaterFunc,eleId){const data={labels:labels,datasets:[{data:dsData,label:dsLabel,minBarLength:5,}]};const config={type:'bar',data:data,options:{responsive:true,plugins:{legend:{display:false,},title:{display:true,text:title}},scales:{y:{beginAtZero:true,ticks:{callback:function(label,index,labels){return formaterFunc(label)}}}}}};const ctx=document.getElementById(eleId);new Chart(ctx,config)};function formatNumber(num){const k=1000;const m=1000000;const b=1000000000;if(num<k){return num}else if(num>k&&num<m){return(num/k).toFixed(1)+'K'}else if(num>=m&&num<=b){return(num/m).toFixed(1)+'M'}else{return(num/b).toFixed(1)+'B'}};function formatBytes(bytes){const kb=1024;const mb=1024*1024;const gb=1024*1024*1024;if(bytes<kb){return bytes.toFixed(1)+'B'}if(bytes>=kb&&bytes<mb){return(bytes/kb).toFixed(1)+'KB'}else if(bytes>=mb&&bytes<=gb){return(bytes/mb).toFixed(1)+'MB'}else{return(bytes/gb).toFixed(1)+'GB'}};function formatExpiry(time){if(time==0){return'Permanent'}else{let date=new Date(time);let year=date.getFullYear();let month=date.getMonth()+1;let day=date.getDate();let hour=date.getHours();let min=date.getMinutes();let second=date.getSeconds();return year+'-'+month+'-'+day+' '+hour+':'+min+':'+second}};</script>";

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
                    res = "Already Expired";
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
                res = "Permanent";
            }

            return res;
        }
    
        internal const char SplitChar = ':';
        internal static string GetShortKey(string key)
        {
            var len = key.Length;

            if(len > 1024)
            {
                var span = key.AsSpan();

                var b = span.Slice(0, 10).ToString();
                var e = span.Slice(len - 6 , 5).ToString();

                var n = $"{b}...({len - 15} more bytes)...{e}";
                return n;
            }

            return key;
        }
    }
}

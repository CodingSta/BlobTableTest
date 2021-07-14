using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Cosmos.Table;

namespace MinGyu.Function
{
    public static class ReadTable
    {
        [FunctionName("ReadTable")]
        public static Task<string> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
        HttpRequest req, ILogger log, ExecutionContext context)
        {
            string connStrA = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string PartitionKeyA = data.PartitionKey;
            string RowKeyA = data.RowKey;

            CloudStorageAccount stoA = CloudStorageAccount.Parse(connStrA);
            CloudTableClient tbC = stoA.CreateCloudTableClient();
            CloudTable tableA = tbC.GetTableReference("tableA");

            string filterA = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, PartitionKeyA);
            string filterB = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, RowKeyA);

            Task<string> response = ReadToTable(tableA, filterA, filterB);
            return response;
        }

        static async Task<string> ReadToTable(CloudTable tableA, string filterA, string filterB)
        {
            TableQuery<MemoData> rangeQ = new TableQuery<MemoData>().Where(
                TableQuery.CombineFilters(filterA, TableOperators.And, filterB)
            );
            TableContinuationToken tokenA = null;
            rangeQ.TakeCount = 10000;
            JArray resultArr = new JArray();
            try
            {
                do
                {
                    TableQuerySegment<MemoData> segment = await tableA.ExecuteQuerySegmentedAsync(rangeQ, tokenA);
                    tokenA = segment.ContinuationToken;
                    foreach (MemoData entity in segment)
                    {
                        JObject srcObj = JObject.FromObject(entity);
                        srcObj.Remove("Timestamp");
                        resultArr.Add(srcObj);
                    }
                } while (tokenA != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                throw;
            }

            string resultA = Newtonsoft.Json.JsonConvert.SerializeObject(resultArr);
            if (resultA != null) return resultA;
            else return "No Data";
        }

        private class MemoData : TableEntity
        {
            public string content { get; set; }
        }
    }
}

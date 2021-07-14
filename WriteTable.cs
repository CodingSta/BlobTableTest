using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos.Table;

namespace MinGyu.Function
{
    public static class WriteTable
    {
        [FunctionName("WriteTable")]
        public static void Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
        HttpRequest req, ILogger log, ExecutionContext context)
        {
            string connStrA = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string PartitionKeyA = data.PartitionKey;
            string RowKeyA = data.RowKey;
            string contentA = data.Content;

            CloudStorageAccount stoA = CloudStorageAccount.Parse(connStrA);
            CloudTableClient tbC = stoA.CreateCloudTableClient();
            CloudTable tableA = tbC.GetTableReference("tableA");

            writeToTable(tableA, contentA, PartitionKeyA, RowKeyA);
        }

        static void writeToTable(CloudTable tableA, string contentA, string PartitionKeyA, string RowKeyA)
        {
            MemoData memoA = new MemoData();
            memoA.PartitionKey = PartitionKeyA;
            memoA.RowKey = RowKeyA;
            memoA.content = contentA;

            TableOperation operA = TableOperation.InsertOrReplace(memoA);
            tableA.Execute(operA);
        }

        private class MemoData : TableEntity
        {
            public string content { get; set; }
        }
    }
}

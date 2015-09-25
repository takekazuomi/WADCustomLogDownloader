using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

namespace WADCustomLogDownloader.WasUtils
{
    public class Table
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly CloudTableClient _tableClient;

        public Table()
        {
            _storageAccount = Account.Get();

            _tableClient = _storageAccount.CreateCloudTableClient();

            _tableClient.DefaultRequestOptions = new TableRequestOptions
            {
                PayloadFormat = TablePayloadFormat.JsonNoMetadata,
                ServerTimeout = new TimeSpan(0, 0, 180),
                MaximumExecutionTime = new TimeSpan(0, 0, 180),
                RetryPolicy = new LinearRetry(new TimeSpan(0, 0, 1), 60)
            };
        }

        public CloudTable GetTableReference(string tableName)
        {
            return _tableClient.GetTableReference(tableName);
        }
    }
}

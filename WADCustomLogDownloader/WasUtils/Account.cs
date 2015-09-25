using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;

namespace WADCustomLogDownloader.WasUtils
{
    public static class Account
    {
        public static CloudStorageAccount Get()
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING") ?? CloudConfigurationManager.GetSetting("StorageConnectionString");

            return CloudStorageAccount.Parse(connectionString);
        }
    }
}

using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace WADCustomLogDownloader.WasUtils
{
    public class Blob
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly CloudBlobClient _blobClient;
        private readonly CloudBlobContainer _blobContainer;


        public Blob(string containerName)
        {
            _storageAccount = Account.Get();

            _blobClient = _storageAccount.CreateCloudBlobClient();

            _blobContainer = _blobClient.GetContainerReference(containerName);

            _blobContainer.CreateIfNotExists();
        }

        public ICloudBlob GetCloudBlob(string blobName, BlobType blobType = BlobType.BlockBlob)
        {
            ICloudBlob cloudBlob;
            switch (blobType)
            {
                case BlobType.AppendBlob:
                    cloudBlob = _blobContainer.GetAppendBlobReference(blobName);
                    break;
                case BlobType.BlockBlob:
                    cloudBlob = _blobContainer.GetBlockBlobReference(blobName);
                    break;
                case BlobType.PageBlob:
                    cloudBlob = _blobContainer.GetPageBlobReference(blobName);
                    break;
                case BlobType.Unspecified:
                    cloudBlob = _blobContainer.GetBlobReferenceFromServer(blobName);
                    break;
                default:
                    throw new ArgumentException($"Invalid blob type {blobType}", nameof(blobType));
            }

            return
                cloudBlob;
        }



    }
}

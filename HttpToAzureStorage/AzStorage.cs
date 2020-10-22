using System;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Files.DataLake;

namespace HttpToAzureStorage
{
    class AzStorage
    {
        public async Task<DataLakeFileClient> Create(string fileName, string storageAccountKey)
        {
            //TODO: Move hardcoded values to config and clean up interface
            const string storageAccountName = "bigblob1000";
            // Documentation claims this API supports Azure AD authentication as well
            
            var serviceUri = new Uri("https://bigblob1000.blob.core.windows.net/");
            var sharedKeyCredential = new StorageSharedKeyCredential(storageAccountName, storageAccountKey);
            var serviceClient = new DataLakeServiceClient(serviceUri, sharedKeyCredential);
            var filesystem = serviceClient.GetFileSystemClient("ccc");
            var exists = await filesystem.ExistsAsync();

            if (!exists.Value)
            {
                await filesystem.CreateAsync();
            }
            var fileClient = filesystem.GetFileClient(fileName);
            await fileClient.DeleteIfExistsAsync();
            await fileClient.CreateIfNotExistsAsync();
            return fileClient;
        }
    }
}
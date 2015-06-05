using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;

namespace ImageCounterWebJob
{
    using System.Globalization;
    using System.Threading;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    // To learn more about Microsoft Azure WebJobs SDK, please see http://go.microsoft.com/fwlink/?LinkID=320976
    public class Program
    {
        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        public static void Main()
        {
            var host = new JobHost();

            host.Call(typeof(Program).GetMethod("ProcessCounts"));
        }

        [NoAutomaticTrigger]
        public static void ProcessCounts()
        {
            var storageAccount =
                new CloudStorageAccount(
                    new StorageCredentials(
                        "abacusdms",
                        "F6D2Y+S4L1F/uOHFapj9hEr4yuUX5wCXf/0nW2NuPdGrlV1VoSD7qMl0yet1QI7O7CX4CP+DkNKtPVLyT+IlGQ=="),
                    false);

            var blobClient = new CloudBlobClient(storageAccount.BlobEndpoint, storageAccount.Credentials);

            var imagesContainer = blobClient.GetContainerReference("gurbetoylaritutanak");
            var allSeenContainer = blobClient.GetContainerReference("gurbetoylaritaranmis");
            allSeenContainer.CreateIfNotExists();

            var metaDataKey = "Timesseen";

            var unprocessedImages = imagesContainer.ListBlobs("tutanakcanavari", true).Where(item => (item is CloudBlockBlob)).ToList();

            var processedImages = unprocessedImages.AsParallel().Where(
                b =>
                    {
                        var blob = b as CloudBlockBlob;
                        if (blob == null)
                        {
                            return false;
                        }

                        blob.FetchAttributes();

                        var timesSeen = blob.Metadata.Keys.Any(k => k == metaDataKey)
                                            ? Convert.ToInt16(blob.Metadata[metaDataKey])
                                            : 0;
                        return timesSeen >= 3;
                    }).ToList();

            var copied = 0;
            foreach (var processedImage in processedImages)
            {
                var existingBlob = processedImage as CloudBlockBlob;
                var name = string.Join("", processedImage.Uri.Segments.Skip(2));
                var newBLob =
                    allSeenContainer.GetBlockBlobReference(name);
                newBLob.StartCopyFromBlob(existingBlob);

                var autoResetEvent = new AutoResetEvent(false);
                var timer = new Timer(
                    s =>
                        {
                            var autoEvent = (AutoResetEvent)s;
                            if (newBLob.CopyState.Status != CopyStatus.Pending)
                            {
                                autoEvent.Set();
                            }
                        }, autoResetEvent, 1000, 500);
                autoResetEvent.WaitOne();
                timer.Dispose();
                if (newBLob.CopyState.Status != CopyStatus.Success)
                {
                    continue;
                }

                if (existingBlob == null)
                {
                    continue;
                }

                existingBlob.Delete();
                copied++;
            }

            var totalProcessed =
                allSeenContainer.ListBlobs("tutanakcanavari", true).Count(item => (item is CloudBlockBlob));
            var remaining = unprocessedImages.Count() - copied;

            var turkishNow = DateTime.UtcNow.ToString("f", new CultureInfo("tr-TR"));
            var resultHtml =
                $@"<p>{turkishNow} (UTC) itibari ile, toplam tutanak: <b>{totalProcessed + remaining
                    }</b></p><p>Taranan tutanaklar: <b><font color = ""green""> {totalProcessed
                    }</font></b></p>  <p>Taranmasi gereken tutanaklar: <b><font color = ""red"">{remaining}</font></b></p>";

            var summaryBlobContainer = blobClient.GetContainerReference("gurbetoylaritutanakmetadata");
            summaryBlobContainer.CreateIfNotExists();

            var summaryBlob = summaryBlobContainer.GetBlockBlobReference("metadata/tutanakcounter.html");
            if (summaryBlob.Exists())
            {
                summaryBlob.Delete();
            }
            summaryBlob.UploadText(resultHtml);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;

namespace ImageCounterWebJob
{
    using System.Data;
    using System.Data.SqlClient;
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

            var unprocessedImages = imagesContainer.ListBlobs("tutanakcanavari", true).Where(item => (item is CloudBlockBlob)).ToList();

            using (var conn = new SqlConnection
            {
                ConnectionString =
                               "Server=tcp:iurk7gqc60.database.windows.net,1433;Database=gurbetinoylari;User ID=gurbetinoylari@iurk7gqc60;Password=Guvenli1SecimOlsun;Trusted_Connection=False;Encrypt=True;Connection Timeout=30;"
            })
            {
                using (var cmd = new SqlCommand("SPC_SAGLAMTUTANAK", conn))
                {
                    var adapter = new SqlDataAdapter(cmd)
                    {
                        SelectCommand =
                                              {
                                                  CommandType =
                                                      System.Data.CommandType
                                                      .StoredProcedure
                                              }
                    };

                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    var images =
                        Enumerable.Select(dataTable.AsEnumerable(), row => row["IMAJ"] as string).Distinct().ToList();


                    var copied = 0;

                    foreach (var image in images)
                    {
                        var imageUri = new Uri(image);
                        var name = string.Join("", imageUri.Segments.Skip(2));
                        if (unprocessedImages.All(b => b.Uri.AbsoluteUri != image))
                        {
                            continue;
                        }

                        var blob = imagesContainer.GetBlockBlobReference(name);

                        if (!blob.Exists())
                        {
                            continue;
                        }

                        var newBlob = allSeenContainer.GetBlockBlobReference(name);
                        newBlob.StartCopyFromBlob(blob);

                        var autoResetEvent = new AutoResetEvent(false);
                        var timer = new Timer(
                            s =>
                            {
                                var autoEvent = (AutoResetEvent)s;
                                if (newBlob.CopyState.Status != CopyStatus.Pending)
                                {
                                    autoEvent.Set();
                                }
                            },
                            autoResetEvent,
                            1000,
                            500);
                        autoResetEvent.WaitOne();
                        timer.Dispose();
                        if (newBlob.CopyState.Status != CopyStatus.Success)
                        {
                            continue;
                        }

                        if (!newBlob.Exists())
                        {
                            continue;
                        }

                        blob.Delete();
                        copied++;
                        var blobUrlToRemove = unprocessedImages.FirstOrDefault(b => b.Uri.AbsoluteUri == image);
                        if (blobUrlToRemove != default(IListBlobItem))
                        {
                            unprocessedImages.Remove(blobUrlToRemove);
                        }

                    }

                    var totalProcessed =
                        allSeenContainer.ListBlobs("tutanakcanavari", true).Count(item => (item is CloudBlockBlob));
                    var remaining = unprocessedImages.Count() - copied;

                    var turkishNow = DateTime.UtcNow.ToString("f", new CultureInfo("tr-TR"));
                    var resultHtml =
                        string.Format(@"<p>{0} (UTC) itibari ile, toplam tutanak: <b>{1}</b></p><p>Taranan tutanaklar: <b><font color = ""green""> {2}</font></b></p>  <p>Taranmasi gereken tutanaklar: <b><font color = ""red"">{3}</font></b></p>",
                            turkishNow, totalProcessed + remaining, totalProcessed, remaining);

                    var summaryBlobContainer = blobClient.GetContainerReference("gurbetoylaritutanakmetadata");
                    summaryBlobContainer.CreateIfNotExists();

                    var summaryBlob = summaryBlobContainer.GetBlockBlobReference("metadata/tutanakcounter.html");
                    if (summaryBlob.Exists())
                    {
                        summaryBlob.Delete();
                    }
                    summaryBlob.UploadText(resultHtml);


                    var urlsBlob = summaryBlobContainer.GetBlockBlobReference("metadata/urls.txt");
                    var urlArray = unprocessedImages.Select(b => b.Uri.AbsoluteUri).ToArray();

                    urlsBlob.UploadText(string.Join(",", urlArray));
                }
            }


        }
    }
}

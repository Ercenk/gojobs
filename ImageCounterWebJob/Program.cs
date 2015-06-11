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
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
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
            ProcessBlobs("gurbetoylaritutanak", "gurbetoylaritaranmis", "tutanakcanavari", "SPC_SAGLAMTUTANAK", CommandType.StoredProcedure, "metadata/tutanakcounter.html", "metadata/urls.txt");
            //            ProcessBlobs("gurbetoylarichp", "gurbetoylarichptaranmis", "", "SELECT DISTINCT  [IMAJ] FROM [dbo].[SANDIKANALIZ] WHERE [IMAJ] IS NOT NULL", CommandType.Text, "metadata/tutanakcounter2.html", "metadata/urlschphdp.txt");
        }

        private static void ProcessBlobs(string fromContainer, string toContainer, string blobPrefix, string sqlCommandText, CommandType commandType, string summaryNote, string blobUriListFile)
        {
            var storageAccount =
                new CloudStorageAccount(
                    new StorageCredentials(
                        "",
                        ""),
                    false);

            var blobClient = new CloudBlobClient(storageAccount.BlobEndpoint, storageAccount.Credentials);

            var imagesContainer = blobClient.GetContainerReference(fromContainer);
            var allSeenContainer = blobClient.GetContainerReference(toContainer);
            allSeenContainer.CreateIfNotExists();

            var unprocessedImages =
                imagesContainer.ListBlobs(blobPrefix, true).Where(item => (item is CloudBlockBlob)).OrderBy(b => b.Uri.AbsoluteUri.Length).ToList();

            using (
                var conn = new SqlConnection
                {
                    ConnectionString =
                                       ""
                })
            {
                using (var cmd = new SqlCommand(sqlCommandText, conn))
                {
                    var adapter = new SqlDataAdapter(cmd)
                    {
                        SelectCommand =
                                              {
                                                  CommandType =
                                                      commandType
                                              }
                    };

                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    var images = Enumerable.Select(dataTable.AsEnumerable(), row => row["IMAJ"] as string).Distinct().ToList();

                    File.WriteAllLines(@"d:\temp\all.txt", images.ToArray());

                    var copied = 0;
                    foreach (var image in images)
                    {
                        if (string.IsNullOrEmpty(image) || image == "dummy")
                        {
                            continue;
                        }
                            Uri imageUri;
                            string name = string.Empty;
    
                                if (!Uri.TryCreate(image, UriKind.Absolute, out imageUri))
                                {
                                    continue;
                                }

                                name = Uri.UnescapeDataString(string.Join("", imageUri.Segments.Skip(2)));

                                if (
                                    unprocessedImages.All(
                                        b =>
                                        Uri.Compare(
                                            b.Uri,
                                            imageUri,
                                            UriComponents.Host | UriComponents.PathAndQuery,
                                            UriFormat.SafeUnescaped,
                                            StringComparison.OrdinalIgnoreCase) != 0))
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
                            Debug.WriteLine(copied);
                            var blobUrlToRemove = unprocessedImages.FirstOrDefault(b => b.Uri.AbsoluteUri == image);
                            if (blobUrlToRemove != default(IListBlobItem))
                            {
                                unprocessedImages.Remove(blobUrlToRemove);
                            }
    
                    }
                    Debug.WriteLine("BITTI");
                    var totalProcessed =
                        allSeenContainer.ListBlobs(blobPrefix, true).Count(item => (item is CloudBlockBlob));
                    var remaining = unprocessedImages.Count() - copied;

                    var turkishNow = DateTime.UtcNow.ToString("f", new CultureInfo("tr-TR"));
                    var resultHtml =
                        string.Format(
                            @"<p>{0} (UTC) itibari ile, toplam tutanak: <b>{1}</b></p><p>Taranan tutanaklar: <b><font color = ""green""> {2}</font></b></p>  <p>Taranmasi gereken tutanaklar: <b><font color = ""red"">{3}</font></b></p>",
                            turkishNow,
                            totalProcessed + remaining,
                            totalProcessed,
                            remaining);

                    var summaryBlobContainer = blobClient.GetContainerReference("gurbetoylaritutanakmetadata");
                    summaryBlobContainer.CreateIfNotExists();

                    var summaryBlob = summaryBlobContainer.GetBlockBlobReference(summaryNote);
                    if (summaryBlob.Exists())
                    {
                        summaryBlob.Delete();
                    }
                    summaryBlob.UploadText(resultHtml);

                    var urlsBlob = summaryBlobContainer.GetBlockBlobReference(blobUriListFile);
                    var urlArray = unprocessedImages.Select(b => b.Uri.AbsoluteUri).ToArray();

                    urlsBlob.UploadText(string.Join(",", urlArray));
                }
            }
        }
    }
}

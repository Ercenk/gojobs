﻿namespace RescrubWebjob
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    // To learn more about Windows Azure WebJobs start here http://go.microsoft.com/fwlink/?LinkID=320976
    public class Program
    {
        // Please set the following connectionstring values in app.config
        // AzureWebJobsDashboard and AzureWebJobsStorage
        public static void Main()
        {
            //var host = new JobHost();
            //host.Call(typeof(Program).GetMethod("ProcessCounts"));
            ProcessCounts();
        }

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

            using (var conn = new SqlConnection
            {
                ConnectionString =
                               "Server=tcp:iurk7gqc60.database.windows.net,1433;Database=gurbetinoylari;User ID=gurbetinoylari@iurk7gqc60;Password=Guvenli1SecimOlsun;Trusted_Connection=False;Encrypt=True;Connection Timeout=30;"
            })
            {
                using (var cmd = new SqlCommand("SPC_FARKLITUTANAK", conn))
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

                    const string metaDataKey = "Timesseen";
                    foreach (var image in images)
                    {
                        var imageUri = new Uri(image);
                        var name = string.Join("", imageUri.Segments.Skip(2));
                        var blob = allSeenContainer.GetBlockBlobReference(name);

                        if (!blob.Exists())
                        {
                            continue;
                        }
                        blob.FetchAttributes();
                        var timesSeen = blob.Metadata.Keys.Any(k => k == metaDataKey)
                                            ? Convert.ToInt16(blob.Metadata[metaDataKey]) - 1
                                            : 0;
                        blob.Metadata[metaDataKey] = timesSeen.ToString();
                        blob.SetMetadata();

                        var newBlob = imagesContainer.GetBlockBlobReference(name);
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
                            }, autoResetEvent, 1000, 500);
                        autoResetEvent.WaitOne();
                        timer.Dispose();
                        if (newBlob.CopyState.Status != CopyStatus.Success)
                        {
                            continue;
                        }

                        blob.Delete();
                    }
                }
            }


        }

       
    }
}
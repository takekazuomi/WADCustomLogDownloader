using Microsoft.WindowsAzure.Storage.Table.Queryable;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using Microsoft.WindowsAzure.Storage.Table;
using Mono.Options;

namespace WADCustomLogDownloader
{
    internal static class Program
    {
        private const string WadDirectoriesTable = "WADDirectoriesTable";
        private static readonly CountdownEvent _countdownEvent = new CountdownEvent(1);
        private static readonly ProgressRecorder _progressRecorder = new ProgressRecorder();
        private static readonly BlockingCollection<List<WadDirectoriesTable>> _jobQueue = new BlockingCollection<List<WadDirectoriesTable>>();
        private static readonly Dictionary<string, WadDirectoriesTable> _sourceFileDictionary = new Dictionary<string, WadDirectoriesTable>();
        private static int _verbose;


        private static void ListWadCustomLogs(DateTime fromDate, DateTime toDate, string container)
        {
            var wasTable = new WasUtils.Table();
            var table = wasTable.GetTableReference(WadDirectoriesTable);

            var fromDateUtc = fromDate.ToUniversalTime();
            var toDateUtc = toDate.ToUniversalTime();

            var query = table.CreateQuery<WadDirectoriesTable>()
                .Where(
                    e =>
                        string.Compare(e.PartitionKey, fromDateUtc.AddHours(-6).Ticks.ToString("d19"), StringComparison.Ordinal) >= 0 &&
                        string.Compare(e.PartitionKey, toDateUtc.AddHours(6).Ticks.ToString("d19"), StringComparison.Ordinal) < 0 &&
                        e.Status.Equals("SUCCEEDED") &&
                        e.FileTime >= fromDateUtc.ToUniversalTime() &&
                        e.FileTime < toDateUtc.ToUniversalTime() &&
                        e.Container == container)
                .AsTableQuery();

            TableContinuationToken currentToken = null;

            do
            {
                var result = query.ExecuteSegmentedAsync(currentToken).Result;
                currentToken = result.ContinuationToken;
                result.Results.ForEach(d => _sourceFileDictionary[d.RelativePath] = d);
                _jobQueue.Add(result.Results);
                WriteVerbose("Add job queue: {0}", result.Count());

            } while (currentToken != null);
            _jobQueue.CompleteAdding();

        }

        private static void DownloadFromBlob(string container, string downloadDir)
        {
            var wasBlob = new WasUtils.Blob(container);

            var context = new TransferContext
            {
                ProgressHandler = _progressRecorder,
                OverwriteCallback = (path, destinationPath) =>
                {
                    var fileinfo = new FileInfo(destinationPath);
                    if (!fileinfo.Exists) return true;

                    var wad = _sourceFileDictionary[destinationPath.Replace(downloadDir + Path.DirectorySeparatorChar, "")];
                    return !(fileinfo.Exists && fileinfo.Length == wad.FileSize && fileinfo.LastWriteTimeUtc == wad.FileTime);
                }
            };

            WriteVerbose("ParallelOperations:{0}", TransferManager.Configurations.ParallelOperations);

            var options = new DownloadOptions {DisableContentMD5Validation = true};

            while (!_jobQueue.IsCompleted)
            {
                var x = _jobQueue.Take();
                foreach (var d in x)
                {
                    _countdownEvent.AddCount();

                    var cloudBlob = wasBlob.GetCloudBlob(d.RelativePath) as CloudBlockBlob;

                    var downloadPath = Path.Combine(downloadDir, d.RelativePath);
                    CreateDirectryIfNotExist(downloadPath);

                    var task = TransferManager.DownloadAsync(cloudBlob, downloadPath, options, context);
                    task.ContinueWith(t =>
                    {
                        if (t.IsFaulted && t.Exception!=null)
                        {
                            // skip and duplicate file comes here
                            var transferErrorCode = t.Exception.InnerExceptions.Select(exception =>
                            {
                                var tex = exception as TransferException;
                                return tex?.ErrorCode ?? TransferErrorCode.None;
                            }).FirstOrDefault();

                            switch (transferErrorCode)
                            {
                                case TransferErrorCode.TransferAlreadyExists:
                                    WriteVerbose("Skip duplicate file to transfer. {0}", d.RelativePath);
                                    break;
                                case TransferErrorCode.NotOverwriteExistingDestination:
                                    WriteVerbose("Alrady download skip to transfer. {0}", d.RelativePath);
                                    break;
                                default:
                                    WriteVerbose("Error occurs when transferring {0}: {1}, {2}", d.RelativePath, transferErrorCode, t.Exception?.ToString());
                                    break;
                            }
                        }
                        else
                        {
                            File.SetLastWriteTimeUtc(downloadPath, d.FileTime);
                            WriteVerbose("Succeed to transfer data. {0}", d.RelativePath);
                        }

                        _countdownEvent.Signal();
                    });
                }
            }
            _countdownEvent.Signal();

        }

        private static void CreateDirectryIfNotExist(string downloadPath)
        {
            var directory = Path.GetDirectoryName(downloadPath);
            if (directory != null && !Directory.Exists(directory))
                Directory.CreateDirectory(directory);
        }

        private static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount*8;
            ServicePointManager.Expect100Continue = false;

            var fromDate = DateTime.Today;
            var toDate = DateTime.Today.AddDays(1);
            var container = "";
            var downloadDir = "";
            var help = false;

            var options = new OptionSet()
            {
                { "f|from=",        "Requred {FROM} is logs local time for download, -f 2015-09-25T08:00:00", v => fromDate = DateTime.Parse(v)},
                { "t|to=",          "Requred {TO} is logs local time (not include), -t 2015-09-26T08:00:00", v => toDate = DateTime.Parse(v)},
                { "c|container=",   "Requred {CONTAINER} is log blob container name", v => container = v},
                { "d|downloadDir=", "Requred {DIR} is local download directory", v => downloadDir = v},
                { "v|verbose",      "verbose output", v => _verbose = v==null ? _verbose : ++_verbose},
                { "debug",          "debug output", v => _verbose = v==null ? _verbose : _verbose+=2},
                { "h|help",         "this message", v => { help = v != null; }}
            };

            try
            {
                options.Parse(args);
                if (help || string.IsNullOrEmpty(container) || string.IsNullOrEmpty(downloadDir))
                {
                    ShowHelp(options);
                    return;
                }
                Console.WriteLine("fromDate:{0:s}, toDate:{1:s}, container:{2}, downloadDir:{3}", fromDate, toDate, container, downloadDir);
            }
            catch (OptionException)
            {
                Console.WriteLine("incorect arguments");
                ShowHelp(options);
                return;
            }

            if (_verbose > 1)
                OperationContext.GlobalRequestCompleted += (sender, args2) =>
                {
                    WriteVerbose("Start:{0}, Elapsed:{1:F2}, {2}, {3}, {4}",
                        args2.RequestInformation.StartTime,
                        (args2.RequestInformation.EndTime - args2.RequestInformation.StartTime).TotalSeconds,
                        args2.Request.Method,
                        args2.Request.RequestUri,
                        args2.RequestInformation.HttpStatusCode);
                };


            Task.Run(() => ListWadCustomLogs(fromDate, toDate, container));
            Task.Run(() => DownloadFromBlob(container, downloadDir));

            _countdownEvent.Wait();

            WriteVerbose(_progressRecorder.ToString());

            Trace.Listeners.Cast<TraceListener>().ToList().ForEach(l => l.Flush());
        }
        static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("Usage: WADCustomLogDownloader [OPTIONS]+ ");
            Console.WriteLine("Download transferd Windows Azure Diagonestics custom log files from Azure Blob.");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
        }
        private static void WriteVerbose(string msg)
        {
            if (_verbose > 0)
                Console.WriteLine(msg);
        }

        [StringFormatMethod("formatStr")]
        private static void WriteVerbose(string format, params object[] args)
        {
            if (_verbose > 0)
                WriteVerbose(string.Format(format, args));
        }

    }

    internal class ProgressRecorder : IProgress<TransferProgress>
    {
        private long _latestBytesTransferred;
        private long _latestNumberOfFilesTransferred;
        private long _latestNumberOfFilesSkipped;
        private long _latestNumberOfFilesFailed;

        public void Report(TransferProgress progress)
        {
            _latestBytesTransferred = progress.BytesTransferred;
            _latestNumberOfFilesTransferred = progress.NumberOfFilesTransferred;
            _latestNumberOfFilesSkipped = progress.NumberOfFilesSkipped;
            _latestNumberOfFilesFailed = progress.NumberOfFilesFailed;
        }

        public override string ToString()
        {
            return $"Transferred bytes: {_latestBytesTransferred}; Transfered: {_latestNumberOfFilesTransferred}, Skipped: {_latestNumberOfFilesSkipped}, Failed: {_latestNumberOfFilesFailed}";
        }
    }

}

using JboxTransfer.Modules;
using System.ComponentModel;
using TboxWebdav.Server.Extensions;
using TboxWebdav.Server.Modules.Tbox.Services;
using Teru.Code.Models;

namespace TboxWebdav.Server.Modules.Tbox
{
    public class TboxUploader
    {
        public Stream stream { get; set; }
        public int maxretrytime { get; set; } = 3;
        public int threadcount { get; set; } = 4;
        public int runningcount { get; set; } = 0;

        private TaskCompletionSource<CommonResult> taskCompletionSource;
        private int count, current;
        long length;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1);

        private readonly TboxService _tbox;
        private readonly TboxUploadSession session;

        public TboxUploader(TboxService tbox)
        {
            _tbox = tbox;
            session = new TboxUploadSession(_tbox);
        }

        public void Init(string targetPath, Stream stream, long length)
        {
            this.stream = stream;
            session.Init(targetPath, length);
            count = length.GetChunkCount();
            this.length = length;
        }

        public CommonResult Run()
        {
            Console.WriteLine("Uploader Started!");
            var res0 = session.PrepareForUpload();
            if (!res0.Success)
                return new CommonResult(false, res0.Message);

            taskCompletionSource = new TaskCompletionSource<CommonResult>();
            for (int i = 0; i < threadcount; i++)
            {
                CheckForNext();
            }
            taskCompletionSource.Task.Wait();
            var res1 = taskCompletionSource.Task.Result;
            Console.WriteLine($"Upload All Done, Result: {res1.success}");
            if (!res1.success)
                return res1;

            var res2 = session.Confirm();
            Console.WriteLine($"Upload Final Done, Result: {res2.Success}");
            if (!res2.Success)
                return new CommonResult(false, res2.Message);
            return new CommonResult(true, "");
        }

        public async void CheckForNext()
        {
            if (current >= count)
            {
                if (runningcount == 0)
                    taskCompletionSource.TrySetResult(new CommonResult(true, ""));
                return;
            }

            _lock.Wait();
            try
            {
                var partNum = session.GetNextPartNumber();
                if (!partNum.Success)
                {
                    throw new Exception(partNum.Message);
                }
                runningcount++;
                current++;
                Console.WriteLine($"Task Chunk {current} Assigned");
                BackgroundWorker bgw = new BackgroundWorker();
                bgw.DoWork += Bgw_DoWork;
                bgw.RunWorkerCompleted += Bgw_RunWorkerCompleted;

                MemoryStream ms = new MemoryStream();
                byte[] buffer = new byte[1024 * 4];
                int bytesRead = 0;
                int bytesToRead = (int)(current == count ? length - (current - 1) * FileSizeExtension.ChunkSize : FileSizeExtension.ChunkSize);
                while ((bytesRead = await stream.ReadAsync(buffer, 0, Math.Min(buffer.Length, bytesToRead))) != 0)
                {
                    ms.Write(buffer, 0, bytesRead);
                    ms.Flush();
                    bytesToRead -= bytesRead;
                }
                ms.Position = 0;

                var args = new TboxUploaderWorkerArgs();
                args.Stream = ms;
                args.Part = partNum.Result;
                args.Retry = maxretrytime;
                bgw.RunWorkerAsync(args);
            }
            catch (Exception ex)
            {
                taskCompletionSource.TrySetResult(new CommonResult(false, ex.Message));
            }
            _lock.Release();
        }

        private void Bgw_RunWorkerCompleted(object? sender, RunWorkerCompletedEventArgs e)
        {
            TboxUploaderWorkerArgs args = (TboxUploaderWorkerArgs)e.Result;
            Console.WriteLine($"Task Chunk {args.Part.PartNumber} {(args.Success ? "Succeeded" : "Failed")}");
            if (!args.Success)
            {
                taskCompletionSource.TrySetResult(new CommonResult(false, "上传失败"));
                return;
            }
            runningcount--;
            CheckForNext();
        }

        private void Bgw_DoWork(object? sender, DoWorkEventArgs e)
        {
            TboxUploaderWorkerArgs args = (TboxUploaderWorkerArgs)e.Argument;
            Console.WriteLine($"Task Chunk {args.Part.PartNumber} Started");
            while (args.Retry-- > 0)
            {
                try
                {
                    var res = session.EnsureNoExpire(args.Part.PartNumber);
                    if (!res.success)
                        continue;
                    var res2 = session.Upload(args.Stream, args.Part.PartNumber);
                    if (!res2.success)
                        continue;
                    session.CompletePart(args.Part);
                }
                catch (Exception ex)
                {
                    continue;
                }
                args.Success = true;
                e.Result = args;
                return;
            }
            args.Success = false;
            e.Result = args;
            return;
        }
    }

    public class TboxUploaderWorkerArgs
    {
        public Stream Stream { get; set; }
        public TboxUploadPartSession Part { get; set; }
        public int Retry { get; set; }
        public bool Success { get; set; }
    }
}

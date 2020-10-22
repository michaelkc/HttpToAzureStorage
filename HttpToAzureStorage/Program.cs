using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Serilog;

namespace HttpToAzureStorage
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .MinimumLevel.Debug()
                .CreateLogger();
            var chunker = new Chunker(new HttpClient(), logger, new ChunkerConfiguration()
            {
                ChunkSizeBytes = 1_000_000
            });

            var azureFile = await new AzStorage().Create("1gb.bin", args[0]);
            logger.Information("Start");
            //var fileUrl = new Uri("http://localhost/1gb.bin");
            //var fileUrl = new Uri("http://localhost/azcopy.exe");
            var fileUrl = new Uri("http://speedtest.novoserve.com/1GB.bin");
            long stopIndex = 0;
            // TODO: Expand to handle exception, e.g. by printing out the index of the last flushed chunk
            //       so it is possible to resume the download from there (will take som work in prepare)
            await chunker.Download(fileUrl, async (response) =>
            {
                logger.Information($"Chunk action start for {response.Index}");
                await using var ms = new MemoryStream(response.Bytes);
                await azureFile.AppendAsync(ms, response.Start);
                if (response.Index % 5 == 0)
                {
                    logger.Information($"Flushing on {response.Index}");
                    await azureFile.FlushAsync(response.Start + response.Bytes.Length);
                    
                }

                stopIndex = response.Stop;
                logger.Information($"Chunk action stop for {response.Index}");
            });
            logger.Information($"End-flushing to {stopIndex}");
            await azureFile.FlushAsync(stopIndex);

            logger.Information("Stop");
        }

    }
}
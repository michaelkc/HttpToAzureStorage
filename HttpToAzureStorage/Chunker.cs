using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Serilog;

namespace HttpToAzureStorage
{
    internal class Chunker
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger _logger;
        private readonly ChunkerConfiguration _chunkerConfiguration;

        public Chunker(HttpClient httpClient, ILogger logger, ChunkerConfiguration chunkerConfiguration)
        {
            _httpClient = httpClient;
            _logger = logger;
            _chunkerConfiguration = chunkerConfiguration;
        }

        public async Task<IObservable<ChunkResponse>> DownloadRx(Uri fileUrl)
        {
            var requests = await PrepareChunkRequests(fileUrl);

            return requests
                .Select(r => Observable.FromAsync(() => GetChunk(r)))
                //.Merge(maxConcurrent: _chunkerConfiguration.MaxConcurrentDownloads);
                .Concat();
        }

        public async Task Download(Uri fileUrl, Func<ChunkResponse, Task> chunkAction)
        {
            var requests = await PrepareChunkRequests(fileUrl);
            var inFlightRequests = new Queue<Task<ChunkResponse>>();
            // Logic
            // Partition requests into batches with size Math.Floor(MaxConcurrentDownloads/2)
            // Start the first batch
            // Start the second batch
            // Await the requests in the first batch in order and raise the events
            // Start the third batch
            // Await the requests in the second batch in order and raise the events
            // Repeat
            int batchSize = (int) Math.Floor((decimal) _chunkerConfiguration.MaxConcurrentDownloads / 2);
            while (requests.Any() || inFlightRequests.Any())
            {
                var numberOfRequestsToStart =
                    inFlightRequests.Any() ? batchSize : batchSize * 2; // Startup

                for (int i = 0; i < numberOfRequestsToStart; i++)
                {
                    if (!requests.Any()) continue;
                    var request = requests.Dequeue();
                    _logger.Information($"In-flighting {request.Index}");
                    // Async methods auto-start
                    var chunkTask = GetChunk(request);
                    inFlightRequests.Enqueue(chunkTask);
                }
                
                for (int i = 0; i < batchSize; i++)
                {
                    if (!inFlightRequests.Any()) continue;
                    var chunkTask = inFlightRequests.Dequeue();
                    var chunkResponse = await chunkTask;
                    _logger.Information($"Await download complete for {chunkResponse.Index}");
                    await chunkAction(chunkResponse);
                    _logger.Information($"Await action complete for {chunkResponse.Index}");
                }
            }

        }

        private async Task<ChunkResponse> GetChunk(ChunkRequest request)
        {
            _logger.Information($"GetChunk {request.Index} start");
            var response = await _httpClient.SendAsync(request.Message);
            response.EnsureSuccessStatusCode();
            var chunkBytes = await response.Content.ReadAsByteArrayAsync();
            _logger.Information($"GetChunk {request.Index} stop");
            return new ChunkResponse
            {
                Start = request.Start,
                Stop = request.Start + chunkBytes.Length,
                Index = request.Index,
                Bytes = chunkBytes
            };
        }

        private async Task<Queue<ChunkRequest>> PrepareChunkRequests(Uri fileUrl)
        {
            _logger.Debug($"PrepareChunkRequests with chunk size {_chunkerConfiguration.ChunkSizeBytes}");
            var headRequest = new HttpRequestMessage { Method = HttpMethod.Head, RequestUri = fileUrl };
            var headResponse = await _httpClient.SendAsync(headRequest);
            headResponse.EnsureSuccessStatusCode();
            var contentLength = long.Parse(headResponse.Content.Headers.GetValues("Content-Length").Single());
            long index = 0;
            long stopOffset;
            var preparedChunkRequests = new Queue<ChunkRequest>();
            do
            {
                var startOffset = index * _chunkerConfiguration.ChunkSizeBytes;
                stopOffset = (index + 1) * _chunkerConfiguration.ChunkSizeBytes - 1;
                var request = new HttpRequestMessage { RequestUri = fileUrl };
                request.Headers.Range = new RangeHeaderValue(startOffset, stopOffset);
                preparedChunkRequests.Enqueue(new ChunkRequest(index, startOffset, stopOffset, request));
                index++;
            }
            while (stopOffset <= contentLength);
            _logger.Debug($"Generated {preparedChunkRequests.Count} chunk requests ");
            return preparedChunkRequests;
        }
    }

    internal class ChunkResponse
    {
        public byte[] Bytes { get; set; }
        public long Start { get; set; }
        public long Stop { get; set; }
        public long Index { get; set; }
    }
}
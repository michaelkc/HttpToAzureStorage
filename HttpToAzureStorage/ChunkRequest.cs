using System.Net.Http;

namespace HttpToAzureStorage
{
    internal class ChunkRequest
    {
        public ChunkRequest(long index, long start, long stop, HttpRequestMessage message)
        {
            Index = index;
            Start = start;
            Stop = stop;
            Message = message;
        }

        public long Index { get; }
        public long Start { get; }
        public long Stop { get; }
        public HttpRequestMessage Message { get; }
    }
}
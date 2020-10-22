namespace HttpToAzureStorage
{
    internal class ChunkerConfiguration
    {
        public long ChunkSizeBytes { get; set; }
        public int MaxConcurrentDownloads { get; set; } = 4;
    }
}
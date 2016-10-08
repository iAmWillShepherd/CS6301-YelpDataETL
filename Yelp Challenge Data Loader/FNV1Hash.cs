namespace YelpDataLoader
{
    public static class FNV1Hash
    {
        private const ulong FNV_OFFSET_BASIS = unchecked(14695981039346656037);

        private const ulong FNV_PRIME = unchecked(1099511628211);

        public static ulong Create(byte[] bytes)
        {
            var hash = FNV_OFFSET_BASIS;

            foreach(var b in bytes)
            {
                hash *= FNV_PRIME;
                hash = hash^b;
            }

            return hash;
        }
    }
}
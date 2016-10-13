namespace YelpDataLoader
{
    public static class Fnv1Hash
    {
        private const ulong FNV_OFFSET_BASIS = 14695981039346656037;

        private const ulong FNV_PRIME = 1099511628211;

        public static ulong Create(byte[] bytes)
        {
            ulong hash = FNV_OFFSET_BASIS;

            foreach (byte b in bytes)
            {
                hash *= FNV_PRIME;
                hash = hash ^ b;
            }

            return hash;
        }
    }
}
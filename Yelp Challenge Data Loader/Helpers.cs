using System.IO;

namespace YelpDataLoader
{
    public static class Helpers
    {
        public static string GetFullFilename(string x) => Path.Combine(basePath, x + ".json");
        private static string basePath => Path.Combine(Directory.GetCurrentDirectory(), "data");
    }
}
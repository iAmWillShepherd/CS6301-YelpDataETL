using System;
using System.Collections.Generic;
using System.IO;

namespace YelpDataLoader
{
    public static class Helpers
    {
        public static string GetFullFilename(string x) => Path.Combine(basePath, x + ".json");
        private static string basePath => Path.Combine(Directory.GetCurrentDirectory(), "data");

        public static IEnumerable<TSource> DistinctBy<TSource, TKey>
            (this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            HashSet<TKey> seenKeys = new HashSet<TKey>();
            foreach (TSource element in source)
            {
                if (seenKeys.Add(keySelector(element)))
                {
                    yield return element;
                }
            }
        }
    }
}
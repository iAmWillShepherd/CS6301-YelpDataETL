using System;
using System.Collections.Generic;
using System.IO;
using MySql.Data.MySqlClient;

namespace YelpDataETL
{
    public static class Helpers
    {
        public static string GetFullFilename(string x) => Path.Combine(BasePath, x + ".json");
        private static string BasePath => Path.Combine(Directory.GetCurrentDirectory(), "data");

        public static MySqlConnection CreateConnectionToYelpDb()
            => new MySqlConnection("server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;");

        public static IEnumerable<TSource> DistinctBy<TSource, TKey>
            (this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            var seenKeys = new HashSet<TKey>();

            foreach (var element in source)
            {
                if (seenKeys.Add(keySelector(element)))
                {
                    yield return element;
                }
            }
        }
    }
}
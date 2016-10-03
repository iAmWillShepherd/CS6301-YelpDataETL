using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace YelpDataLoader
{
    public class Program
    {
        private static MySqlConnection connection => new MySqlConnection("server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;");

        public static void Main(string[] args)
        {
            var loaders = new List<Task> {
                Task.Run(() => BusinessLoader.Load(connection)),
                Task.Run(() => CheckinLoader.Load(connection)),
                Task.Run(() => ReviewLoader.Load(connection)),
                Task.Run(() => TipLoader.Load(connection)),
                Task.Run(() => UserLoader.Load(connection))
            };

            Task.WaitAll(loaders.ToArray());

            Console.WriteLine("Done.");
        }
    }
}

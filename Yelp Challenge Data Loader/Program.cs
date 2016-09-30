using System;
using MySql.Data.MySqlClient;

namespace YelpDataLoader
{
    public class Program
    {
        private static MySqlConnection connection => new MySqlConnection("server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;");

        public static void Main(string[] args)
        {
            BusinessLoader.Load(connection);

            Console.WriteLine("Completed loading...");
        }
    }
}

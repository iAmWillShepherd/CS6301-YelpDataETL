using System;
using MySql.Data.MySqlClient;
using Dapper;
using System.IO;
using Newtonsoft.Json;
using System.Linq;
using System.Collections.Generic;

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

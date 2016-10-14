using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using YelpDataETL.Loaders;

namespace YelpDataETL
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var conn = new MySqlConnection("server=localhost;user=root;password=P@ssword!;port=3306;");

            Console.WriteLine("Creating database: yelp...");
            
            conn.Open();

            try
            {
                conn.Execute("DROP DATABASE IF EXISTS yelp");
                conn.Execute(File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), "data", "yelp_db.sql")));
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }

            Console.WriteLine("Database created.");
            Console.WriteLine("Staring load...");

            var loaders = new List<Task> {
                Task.Run(() => BusinessLoader.Load(Helpers.CreateConnectionToYelpDb())),
                Task.Run(() => CheckinLoader.Load(Helpers.CreateConnectionToYelpDb())),
                Task.Run(() => ReviewLoader.Load(Helpers.CreateConnectionToYelpDb())),
                Task.Run(() => TipLoader.Load(Helpers.CreateConnectionToYelpDb())),
                Task.Run(() => UserLoader.Load(Helpers.CreateConnectionToYelpDb()))
            };

            try
            {
                Task.WaitAll(loaders.ToArray());
                Console.WriteLine("Load complete.");
            }
            catch (AggregateException aEx)
            {
                Console.WriteLine("Loaded Failed.");

                foreach (var ex in aEx.Flatten().InnerExceptions)
                {
                    Console.WriteLine(ex.Message);    
                }
            }          
        }
    }
}
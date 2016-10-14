using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
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
            var conn = new MySqlConnection("server=localhost;user=yelp_etl;password=P@ssword!;port=3306;");

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

            //Long running task should start first
            var loaders = new List<Task> {
                Task.Run(() => BusinessLoader.Load(Helpers.CreateConnectionToYelpDb())),                
                Task.Run(() => ReviewLoader.Load(Helpers.CreateConnectionToYelpDb())),             
            };

            //Faster loaders can run sequencially
            CheckinLoader.Load(Helpers.CreateConnectionToYelpDb());
            TipLoader.Load(Helpers.CreateConnectionToYelpDb());
            UserLoader.Load(Helpers.CreateConnectionToYelpDb());            

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

                Console.WriteLine("Dropping database: yelp...");

                conn.Execute("DROP DATABASE IF EXISTS yelp");

                Console.WriteLine("Database dropped.");
            }          
        }
    }
}
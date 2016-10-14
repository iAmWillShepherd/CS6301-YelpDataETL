using System;
using System.Collections.Generic;
using System.Data;
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

            try
            {
                DropDatabaseIfExist(conn);
                conn.Open();
                conn.Execute(File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), "data", "yelp_db.sql")));            
            }
            finally
            {
                conn.Close();
            }

            Console.WriteLine("Database created.");

            try
            {
                Console.WriteLine("Starting load...");

                //Long running task should start first
                var longRunningTask = Task.Run(() => BusinessLoader.Load(Helpers.CreateConnectionToYelpDb()))
                    .ContinueWith((antecedant) => ReviewLoader.Load(Helpers.CreateConnectionToYelpDb()));

                longRunningTask.Start();

                //Faster loaders can run sequencially
                CheckinLoader.Load(Helpers.CreateConnectionToYelpDb());
                TipLoader.Load(Helpers.CreateConnectionToYelpDb());
                UserLoader.Load(Helpers.CreateConnectionToYelpDb());


                longRunningTask.Wait();
                Console.WriteLine("Load complete.");
            }
            catch (AggregateException aEx)
            {
                Console.WriteLine("Load Failure.");

                foreach (var ex in aEx.Flatten().InnerExceptions)
                {
                    Console.WriteLine(ex.Message);    
                }

                DropDatabaseIfExist(conn);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Load Failure: {ex.Message}.");
                DropDatabaseIfExist(conn);
            }

            conn.Dispose();

            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }

        private static void DropDatabaseIfExist(IDbConnection conn)
        {
            conn.Open();

            Console.WriteLine("Dropping database: yelp...");

            conn.Execute("DROP DATABASE IF EXISTS yelp");

            Console.WriteLine("Database dropped.");

            conn.Close();
        }
    }
}
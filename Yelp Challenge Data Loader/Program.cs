using System;
using MySql.Data.MySqlClient;
using Dapper;
using System.IO;
using Newtonsoft.Json;
using System.Linq;

namespace ConsoleApplication
{
    public class Program
    {
        private static string basePath => Path.Combine(Directory.GetCurrentDirectory(), "data");
        private static MySqlConnection connection => new MySqlConnection("server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;");

        public static void Main(string[] args)
        {
            LoadTips(connection);

            Console.WriteLine("Completed loading...");
        }

        private static string GetFullFilename(string x) => Path.Combine(basePath, x + ".json");

        private static void LoadReviews(MySqlConnection connection)
        {
            var sql = 
                @"INSERT INTO review (
                    business_id,
                    user_id,
                    stars,
                    text,
                    date,
                    votes_funny,
                    votes_useful,
                    votes_cool)
                VALUES (
                    @business_id,
                    @user_id,
                    @stars,
                    @text,
                    @date,
                    @votes_funny,
                    @votes_useful,
                    @votes_cool);";

            var objs = File
                .ReadLines(GetFullFilename("review"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x => {
                    dynamic obj = x;

                    return new {
                        obj.business_id,
                        obj.user_id,
                        obj.stars,
                        obj.text,
                        obj.date,
                        votes_funny = obj.votes?.funny,
                        votes_useful = obj.votes?.useful,
                        votes_cool = obj.votes?.cool
                    };
                });

            connection.Open();

            try
            {
                connection.Execute(sql, objs);		
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }	
        }

        private static void LoadTips(MySqlConnection connection)
        {
            var sql = 
                @"INSERT INTO tip (
                    text,
                    business_id,
                    user_id,
                    date,
                    likes)
                VALUES (
                    @text,
                    @business_id,
                    @user_id,
                    @date,
                    @likes);";

            var objs = File
                .ReadLines(GetFullFilename("tip"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x => {
                    dynamic obj = x;

                    return new {
                        obj.text,
                        obj.business_id,
                        obj.user_id,
                        obj.date,
                        obj.likes
                    };
                });

                connection.Open();

                try
                {
                    connection.Execute(sql, objs);
                }
                finally
                {
                    connection.Close();
                    connection.Dispose();
                }
        }

        private static void LoadBusinesses()
        {

        }

        private static void LoadUsers()
        {

        }

        private static void LoadCheckins()
        {

        }
    }
}

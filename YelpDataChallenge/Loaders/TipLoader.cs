using System;
using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using Dapper;

namespace YelpDataETL.Loaders
{
    public class TipLoader
    {
        private static string _sql =
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

        public static void Load(IDbConnection connection)
        {
            Console.WriteLine("Loading tips...");

            var objs = File
                .ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_tip"))
                .Select(JsonConvert.DeserializeObject)
                .Select(x =>
                {
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

            var transaction = connection.BeginTransaction();

            try
            {
                connection.Execute(_sql, objs, transaction);
                transaction.Commit();
            }
            catch (Exception)
            {
                transaction.Rollback();
                throw;
            }
            finally
            {
                transaction.Dispose();
                connection.Close();
                connection.Dispose();
            }

            Console.WriteLine("Completed loading tips.");
        }
    }
}
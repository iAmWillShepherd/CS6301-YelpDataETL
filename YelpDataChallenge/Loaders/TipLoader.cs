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
                business_id,
                user_id,
                text,
                date,
                likes)
            VALUES (
                @business_id,
                @user_id,
                @text,
                @date,
                @likes);";

        public static void Load(IDbConnection connection)
        {
            Console.WriteLine($"{nameof(TipLoader)} - Starting load...");

            var objs = File
                .ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_tip"))
                .Select(JsonConvert.DeserializeObject)
                .Select(x => {
                    dynamic obj = x;

                    return new {
                        obj.business_id,
                        obj.user_id,
                        text = (string)null,
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

                Console.WriteLine($"{nameof(TipLoader)} - Load complete.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{nameof(TipLoader)} - ERROR: {ex.Message}. Rolling back...");
                transaction.Rollback();
                Console.WriteLine($"{nameof(TipLoader)} - Rollback complete.");

                throw;
            }
            finally
            {
                transaction.Dispose();
                connection.Close();
                connection.Dispose();
            }
        }
    }
}
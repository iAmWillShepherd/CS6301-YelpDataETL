using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using Dapper;

namespace YelpDataETL.Loaders
{
    public class ReviewLoader
    {
        private static string _sql =
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

        public static void Load(IDbConnection connection)
        {
            Console.WriteLine($"{nameof(ReviewLoader)} - Starting load...");

            var objs = File
                .ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_review"))
                .Select(JsonConvert.DeserializeObject)
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

            var transaction = connection.BeginTransaction();

            try
            {
                connection.Execute(_sql, objs, transaction);

                transaction.Commit();
                Console.WriteLine($"{nameof(ReviewLoader)} - Load complete.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{nameof(ReviewLoader)} - ERROR: {ex.Message}. Rolling back...");
                transaction.Rollback();
                Console.WriteLine($"{nameof(ReviewLoader)} - Rollback complete.");

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
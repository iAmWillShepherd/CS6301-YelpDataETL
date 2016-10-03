using System;
using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using Dapper;

namespace YelpDataLoader
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
            var objs = File
                .ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_review"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x =>
                {
                    dynamic obj = x;

                    return new
                    {
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
                connection.Execute(_sql, objs);
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }

            Console.WriteLine("Completed loading review data.");
        }
    }
}
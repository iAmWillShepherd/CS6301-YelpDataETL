using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using Dapper;

namespace YelpDataLoader
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
            var objs = File
                .ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_tip"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x =>
                {
                    dynamic obj = x;

                    return new
                    {
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
                connection.Execute(_sql, objs);
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
        }
    }
}
using System;
using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;
using Dapper;

namespace YelpDataLoader
{
    public class BusinessLoader
    {
        private string businessSql =
            @"INSERT INTO business (
                business_id,
                name,
                full_address,
                city,
                state,
                latitude,
                longitude,
                stars,
                review_count,
                open)
            VALUES (
                @business_id,
                @name,
                @full_address,
                @city,
                @state,
                @lat,
                @long,
                @stars,
                @review_count,
                @open);";

        private string categorySql =
            @"INSERT INTO category (
                business_id,
                category)
            VALUES (
                @business_id,
                @category);";

        private string hours =
            @"INSERT INTO hours (
                business_id,
                day,
                close,
                open)
            VALUES (
                @business_id,
                @day,
                @close,
                @open);";

        public static void Load(IDbConnection connection)
        {
            var objs = File.ReadLines(Helpers.GetFullFilename("business"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x => {
                    dynamic obj = x;

                    //Build deserialized object
                    var record = new {
                        business = new {
                            obj.business_id,
                            obj.name,
                            obj.full_address,
                            obj.city,
                            obj.state,
                            obj.latitude,
                            obj.longitude,
                            obj.stars,
                            obj.review_count,
                            obj.open
                        },
                        categories = new List<string>(),
                        hours = new Dictionary<string, Tuple<string, string>>()
                    };

                    //TODO: Map categories, hours, and attributes

                    return record;
                });

            connection.Open();
            
            var transaction = connection.BeginTransaction();

            try
            {
                //TODO: Implement SQL insertion logic

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                Console.WriteLine(ex.Message);

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
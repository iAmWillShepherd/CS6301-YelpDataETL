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
            var categories = new HashSet<string>();
            var attributes = new HashSet<string>();

            var objs = File.ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_business"))
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
                        hours = new Dictionary<string, Tuple<string, string>>()
                    };

                    foreach (var category in obj.categories)
                    {
                        categories.Add(DatabaseStringify(category.ToString()));
                    }

                    return record;
                });

            connection.Open();
            
            var transaction = connection.BeginTransaction();

            try
            {
                var a = objs.ToList();

                //Create category table
                string cols = string.Join(",", categories.Select(x => x + " SMALLINT NULL DEFAULT 0"));
                var createTableScript = string.Concat($"CREATE TABLE business_category ( business_id NVARCHAR(45) NOT NULL, { cols }, PRIMARY KEY(business_id));");

                using(var fs = File.CreateText("test_script.sql"))
                {
                    fs.Write(createTableScript);
                }
                //Console.WriteLine(createTableScript);
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

            Console.WriteLine("Completed loading business data...");
        }

        private static string DatabaseStringify(string str)
        {
            string result = str.ToLower().Trim();

            if (result.Contains("("))
            {
                result = result.Replace("(", " ")
                    .Replace(")", "");
            }

            if (result.Contains("&"))
            {
                result = result.Replace("&", "and");
            }

            if (result.Contains("'"))
            {
                result = result.Replace("'", "");
            }

            if (result.Contains("/"))
            {
                result = result.Replace("/", "");
            }

            if (result.Contains("-"))
            {
                result = result.Replace("-", "");
            }

            if (result.Contains(","))
            {
                result = result.Replace(",", "");
            }
            
            if (result.Contains(" "))
            {
                result = result.ToLower()
                    .Replace(" ", "_");
            }

            return result;
        }
    }
}
using System;
using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;
using Dapper;
using MySql.Data.MySqlClient;

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
                        categories.Add(category.ToString());
                    }

                    return record;
                }).ToList();

            connection.Open();
            //objs.Take(10).ToList();

            try
            {
                //Create category table
                var partitionedCategories = new Dictionary<int, List<string>>();

                for (var i = 0; i < 10; i++)
                {
                    partitionedCategories.Add(i, new List<string>());
                }

                foreach (var category in categories)
                {
                    var idx = FNV1Hash.Create(Encoding.ASCII.GetBytes(category)) % (ulong)partitionedCategories.Count;

                    partitionedCategories[(int)idx].Add(category); 
                }

                var categoryTables = new List<string>();

                for (var i = 0; i < partitionedCategories.Count; i++)
                {
                    var columns = string.Join(", ", 
                        partitionedCategories[i].Select(x => DatabaseStringify(x) + $" SMALLINT NULL DEFAULT 0"));

                    var createSql = $"DROP TABLE IF EXISTS business_category_{i + 1}; CREATE TABLE business_category_{i + 1} ( business_id NVARCHAR(45) NOT NULL, { columns }, PRIMARY KEY(business_id));";
                    
                    connection.Execute(createSql);
                }
            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
            
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

            Console.WriteLine("Completed loading business data...");
        }

        private static string DatabaseStringify(string str)
        {
            var result = str.ToLower()
                .Replace("(", " ")
                .Replace("-", " ")
                .Replace(")", "")
                .Replace("'", "")
                .Replace(",", "")
                .Replace("/", " ")
                .Replace("&", "and")
                .Replace(" ", "_");
               
            return result;
        }
    }
}
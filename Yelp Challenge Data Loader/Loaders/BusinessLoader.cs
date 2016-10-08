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

        private string categorySqlFormat =
            @"INSERT INTO business_category_{0} (
                business_id,
                {1})
            VALUES (
                @business_id,
                {2});";

        private string attributeSqlFormat =
            @"INSERT INTO business_attribute_{0} (
                business_id,
                {1})
            VALUES (
                @business_id,
                {2});";

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
                        businessCategories = new List<string>(),
                        businessAttributes = new List<KeyValuePair<string, string>>(),
                        hours = new Dictionary<string, Tuple<string, string>>()
                    };

                    foreach (var category in obj.categories)
                    {
                        record.businessCategories.Add(category.ToString());
                    }

                    return record;
                });

            connection.Open();

            var transaction = connection.BeginTransaction();

            try
            {   
                foreach (var x in objs.SelectMany(x => x.businessCategories))
                {
                    categories.Add(x);
                }

                foreach (var x in objs.SelectMany(x => x.businessAttributes))
                {

                }

                foreach (var script in BuildAtrributeTables(attributes)
                    .Union(BuildCategoryTables(categories)
                    .Where(x => !string.IsNullOrEmpty(x))))
                {
                    connection.Execute(script);
                }

                //TODO: Implement SQL insertion logic
                foreach (var obj in objs)
                {
                    foreach (var insertScript in BuildCategoryInsertSql(obj.businessCategories))
                    {
                        connection.Execute(insertScript, transaction);
                    }
                }

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

        private static List<string> BuildCategoryTables(IEnumerable<string> categories)
        {
            var partitionedCategories = Partition(categories);
            var categoryTables = new List<string>();

            for (var i = 0; i < partitionedCategories.Count; i++)
            {
                var columns = string.Join(", ", 
                    partitionedCategories[i].Select(x => DatabaseStringify(x) + $" SMALLINT NULL DEFAULT 0"));

                categoryTables.Add($"DROP TABLE IF EXISTS business_category_{i + 1}; CREATE TABLE business_category_{i + 1} ( business_id NVARCHAR(45) NOT NULL, { columns }, PRIMARY KEY(business_id));");
            }

            return categoryTables;
        }

        private static List<string> BuildAtrributeTables(IEnumerable<string> attributes)
        {
            var attributeTables = new List<string>();

            return attributeTables;
        }

        private static List<string> BuildCategoryInsertSql(List<string> categories)
        {
            var partitionedCategories = Partition(categories);
            var categoryInserts = new List<string>();

            for (int i = 0; i < partitionedCategories.Count; i++)
            {
                
            }

            return categoryInserts;
        }

        private static Dictionary<int, List<string>> Partition(IEnumerable<string> xs, int numberOfPartitions = 10)
        {
            var partitions = new Dictionary<int, List<string>>();

            for (var i = 0; i < numberOfPartitions; i++)
            {
                partitions.Add(i, new List<string>());
            }

            foreach (var x in xs)
            {
                var idx = FNV1Hash.Create(Encoding.ASCII.GetBytes(x)) % (ulong)partitions.Count;

                partitions[(int)idx].Add(x); 
            }

            return partitions;
        }
    }
}
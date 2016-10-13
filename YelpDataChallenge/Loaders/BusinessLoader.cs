using System;
using System.Collections;
using System.Data;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Dapper;
using Newtonsoft.Json.Linq;

namespace YelpDataETL.Loaders
{
    public class BusinessLoader
    {
        private static string InsertBusinessSql =>
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

        private static string InsertCategorySqlFormat =>
            @"INSERT INTO business_category_{0} (
                business_id,
                {1})
            VALUES (
                @business_id,
                {2});";

        private static string InsertAttributeSqlFormat =>
            @"INSERT INTO business_attribute_{0} (
                business_id,
                {1})
            VALUES (
                @business_id,
                {2});";

        private static string InsertHoursSql =>
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
            var records = File.ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_business"))
                .Select(JObject.Parse)
                .Select(x => {
                    //Build deserialized object
                    var record = new Business {
                        BusinessId = (string)x["business_id"],
                        Name = (string)x["name"],
                        FullAddress = (string)x["full_address"],
                        City = (string)x["jType.city"],
                        State = (string)x["state"],
                        Latitude =  x["latitude"] == null ? default(float) : (float)x["latitude"],
                        Longitude = x["longitude"] == null? default(float) : (float)x["longitude"],
                        Stars = x["stars"] == null ? default(float) : (float)x["stars"],
                        ReviewCount = x["review_count"] == null ? 0 : (int)x["review_count"],
                        Open = (bool)x["open"]
                    };

                    foreach (var category in x["categories"].Children())
                    {
                        record.Categories.Add(category.ToString());
                    }

                    foreach (var attribute in x["attributes"].Children())
                    {
                        var property = ((JProperty) attribute);
                        var propertyType = GetClrType(property.Value.Type);
                        
                        if (propertyType == typeof(IEnumerable))
                        {
                            
                        }
                        else
                        {
                            record.Attributes.Add(new BusinessAttributeInfo {
                                Path = attribute.Path,
                                Key = property.Name.ToLower(),
                                Value = property.Value.ToString(),
                                ValueType = propertyType
                            });
                        }

                    }

                    foreach (var hour in x["hours"])
                    {
                        
                    }

                    return record;
                });

            connection.Open();

            var transaction = connection.BeginTransaction();

            try
            {
                var businesses = records as IList<Business> ?? records.ToList();

                var attributes = businesses
                    .SelectMany(x => x.Attributes)
                    .DistinctBy(y => y.Key);

                var categories = businesses
                    .SelectMany(x => x.Categories)
                    .Distinct();

                var sqlScripts = BuildAtrributeTables(attributes).Union(BuildCategoryTables(categories));

                foreach (string script in sqlScripts)
                {
                    connection.Execute(script);
                }

                foreach (var record in businesses)
                {
                    //Insert categories
                    foreach (string insertScript in BuildCategoryInsertSql(record.Categories))
                    {
                        connection.Execute(insertScript, new {
                            business_id = record.BusinessId
                        }, transaction); 
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

        private static IEnumerable<string> BuildCategoryTables(IEnumerable<string> categories)
        {
            var partitionedCategories = Partition(categories);
            var categoryTables = new List<string>();

            for (var i = 0; i < partitionedCategories.Count; i++)
            {
                string columns = string.Join(", ",
                    partitionedCategories[i].Select(x => DatabaseStringify(x) + $" SMALLINT NULL DEFAULT 0"));

                categoryTables.Add(
                    $"DROP TABLE IF EXISTS business_category_{i + 1}; CREATE TABLE business_category_{i + 1} ( business_id NVARCHAR(45) NOT NULL, {columns}, PRIMARY KEY(business_id));");
            }

            return categoryTables;
        }

        private static IEnumerable<string> BuildAtrributeTables(IEnumerable<BusinessAttributeInfo> attributes)
        {
            var businessAttributeInfos = attributes as IList<BusinessAttributeInfo> ?? attributes.ToList();

            var objs = businessAttributeInfos.Where(attr => attr.ValueType == typeof(object)).ToList();            
            var groupedAttributes = businessAttributeInfos.GroupBy(x => x.Key);
            //var partitionedAttributes = Partition(attributes);
            var attributeTables = new List<string>();

            return attributeTables;
        }

        private static IEnumerable<string> BuildCategoryInsertSql(IEnumerable<string> categories)
        {
            var partitionedCategories = Partition(categories);
            var categoryInserts = new List<string>();

            for (var i = 0; i < partitionedCategories.Count; i++)
            {
                if (partitionedCategories[i].Count <= 0) continue;

                string colsToInsert = string.Join(", ", partitionedCategories[i].Select(DatabaseStringify));
                string valsToInsert = string.Join(", ", Enumerable.Repeat(true, partitionedCategories[i].Count));
                string sql = string.Format(InsertCategorySqlFormat, i + 1, colsToInsert, valsToInsert);

                categoryInserts.Add(sql);
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

            foreach (string x in xs)
            {
                ulong idx = Fnv1Hash.Create(Encoding.ASCII.GetBytes(x)) % (ulong)partitions.Count;

                partitions[(int)idx].Add(x); 
            }

            return partitions;
        }

        private static Type GetClrType(JTokenType jType)
        {
            switch (jType)
            {
                case JTokenType.Boolean:
                    return typeof(bool);
                case JTokenType.String:
                    return typeof(string);
                case JTokenType.Integer:
                    return typeof(int);
                default:
                    return typeof(IEnumerable);
            }
        }

        private static string DatabaseStringify(string str)
        {
            string result = str
                .Trim()
                .ToLower()
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
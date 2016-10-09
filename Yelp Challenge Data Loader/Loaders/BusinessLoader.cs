using System;
using System.Data;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;
using Dapper;

namespace YelpDataLoader
{
    public class AttributeInfo
    {
        public string Path { get; set; }

        public string Key { get; set; }

        public string Value { get; set; }

        public override bool Equals (object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            
            return (Key == ((AttributeInfo)obj).Key);
        }
        
        // override object.GetHashCode
        public override int GetHashCode()
        {
           return Key.GetHashCode() ^ Path.GetHashCode();
        }
    }

    public class Business
    {
        public string BusinessId { get; set; }
        public string Name { get; set; }
        public string FullAddress { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public float Latitude { get; set; }
        public float Longitude { get; set; }
        public float Stars { get; set; }
        public int ReviewCount { get; set; }
        public bool Open { get; set; }
        public List<string> Categories { get; set; }
        public List<AttributeInfo> Attributes { get; set; }
        public Dictionary<string, KeyValuePair<string, string>> Hours { get; set; }

        public Business()
        {
            Categories = new List<string>();
            Attributes = new List<AttributeInfo>();
            Hours = new Dictionary<string, KeyValuePair<string, string>>();
        }
    }

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
            var categories = new HashSet<string>();
            var attributes = new HashSet<AttributeInfo>();

            var records = File.ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_business"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x => {
                    dynamic obj = x;

                    //Build deserialized object
                    var record = new Business {
                        BusinessId = obj.business_id,
                        Name = obj.name,
                        FullAddress = obj.full_address,
                        City = obj.city,
                        State = obj.state,
                        Latitude = obj.latitude,
                        Longitude = obj.longitude,
                        Stars = obj.stars,
                        ReviewCount = obj.review_count,
                        Open = obj.open
                    };

                    foreach (var category in obj.categories)
                    {
                        record.Categories.Add(category.ToString());
                    }

                    foreach (var attribute in obj.attributes)
                    {
                        record.Attributes.Add(new AttributeInfo {
                            Path = attribute.Path,
                            Key = attribute.Name,
                            Value = attribute.Value.ToString()
                        });

                        if (attribute.Type == Newtonsoft.Json.Linq.JTokenType.Object)
                        {
                            foreach (var child in attribute.ChildrenTokens)
                            {
                                record.Attributes.Add(new AttributeInfo {
                                    Path = child.Path,
                                    Key = child.Key,
                                    Value = child.Value.ToString()
                                });
                            }
                        }
                    }

                    return record;
                });

            connection.Open();

            var transaction = connection.BeginTransaction();

            try
            {   
                foreach (var category in records.SelectMany(x => x.Categories))
                {
                    categories.Add(category);
                }

                foreach (var attributeInfo in records.SelectMany(x => x.Attributes))
                {
                    attributes.Add(attributeInfo);
                }

                foreach (var script in BuildAtrributeTables(attributes)
                    .Union(BuildCategoryTables(categories)))
                {
                    connection.Execute(script);
                }

                foreach (var record in records)
                {
                    //Insert categories
                    foreach (var insertScript in BuildCategoryInsertSql(record.Categories))
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

        private static string DatabaseStringify(string str)
        {
            var result = str
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

        private static List<string> BuildAtrributeTables(HashSet<AttributeInfo> attributes)
        {
            var groupedAttributes = attributes.GroupBy(x => x.Key.ToLower());

            //var partitionedAttributes = Partition(attributes);
            var attributeTables = new List<string>();

            // for (var i = 0; i < partitionedAttributes.Count; i++)
            // {
            //     // var columns = string.Join(", ",
            //     //     partitionedAttributes[i].Select(x => ""));
            // }

            return attributeTables;
        }

        private static List<string> BuildCategoryInsertSql(IEnumerable<string> categories)
        {
            var partitionedCategories = Partition(categories);
            var categoryInserts = new List<string>();

            for (int i = 0; i < partitionedCategories.Count; i++)
            {
                if (partitionedCategories[i].Count > 0)
                {
                    var colsToInsert = string.Join(", ", partitionedCategories[i].Select(x => DatabaseStringify(x)));
                    var valsToInsert = string.Join(", ", Enumerable.Repeat(true, partitionedCategories[i].Count));
                    var sql = string.Format(InsertCategorySqlFormat, i + 1, colsToInsert, valsToInsert);

                    categoryInserts.Add(sql);
                }
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
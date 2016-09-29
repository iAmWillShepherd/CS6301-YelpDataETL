using System;
using MySql.Data.MySqlClient;
using Dapper;
using System.IO;
using Newtonsoft.Json;
using System.Linq;
using System.Collections.Generic;

namespace ConsoleApplication
{
    public class Program
    {
        private static string basePath => Path.Combine(Directory.GetCurrentDirectory(), "data");
        private static MySqlConnection connection => new MySqlConnection("server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;");

        public static void Main(string[] args)
        {
            LoadCheckins(connection);

            Console.WriteLine("Completed loading...");
        }

        private static string GetFullFilename(string x) => Path.Combine(basePath, x + ".json");

        private static void LoadReviews(MySqlConnection connection)
        {
            var sql = 
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

            var objs = File
                .ReadLines(GetFullFilename("review"))
                .Select(x => JsonConvert.DeserializeObject(x))
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

            try
            {
                connection.Execute(sql, objs);		
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }	
        }

        private static void LoadTips(MySqlConnection connection)
        {
            var sql = 
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

            var objs = File
                .ReadLines(GetFullFilename("tip"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x => {
                    dynamic obj = x;

                    return new {
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
                    connection.Execute(sql, objs);
                }
                finally
                {
                    connection.Close();
                    connection.Dispose();
                }
        }

        private static void LoadBusinesses()
        {

        }

        private static void LoadUsers()
        {

        }

        private static void LoadCheckins(MySqlConnection connection)
        {
            var sql =
                @"INSERT INTO yelp.checkin (
                    business_id,
                    day_of_week_id,
                    hour_0,
                    hour_1,
                    hour_2,
                    hour_3,
                    hour_4,
                    hour_5,
                    hour_6,
                    hour_7,
                    hour_8,
                    hour_9,
                    hour_10,
                    hour_11,
                    hour_12,
                    hour_13,
                    hour_14,
                    hour_15,
                    hour_16,
                    hour_17,
                    hour_18,
                    hour_19,
                    hour_20,
                    hour_21,
                    hour_22,
                    hour_23)
                VALUES (
                    @business_id,
                    @day_of_week_id,
                    @hour_0,
                    @hour_1,
                    @hour_2,
                    @hour_3,
                    @hour_4,
                    @hour_5,
                    @hour_6,
                    @hour_7,
                    @hour_8,
                    @hour_9,
                    @hour_10,
                    @hour_11,
                    @hour_12,
                    @hour_13,
                    @hour_14,
                    @hour_15,
                    @hour_16,
                    @hour_17,
                    @hour_18,
                    @hour_19,
                    @hour_20,
                    @hour_21,
                    @hour_22,
                    @hour_23);";

            var objs = File.ReadLines(GetFullFilename("checkin"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x => {
                    dynamic obj = x;

                    var record = new {
                        obj.business_id,
                        checkinInfo = new Dictionary<int, List<int>>()
                    };

                    for (int i = 0; i < 7; i++)
                    {
                        record.checkinInfo.Add(i, new int[24].ToList());
                    }

                    var arr = new int[24];

                    foreach (var info in obj.checkin_info)
                    {
                         var key = info.Name;
                         var tmp = key.Split('-');
                         var hour = int.Parse(tmp[0]);
                         var day = int.Parse(tmp[1]);
                         var count = (int)info.Value.Value;

                         record.checkinInfo[day][hour] = count;
                    }

                    return record;
                });

                connection.Open();

                try
                {
                    foreach (var obj in objs)
                    {
                        foreach (var kvp in obj.checkinInfo)
                        {
                            connection.Execute(sql, new {
                                obj.business_id,
                                day_of_week_id = kvp.Key,
                                hour_0 = kvp.Value[0],
                                hour_1 = kvp.Value[1],
                                hour_2 = kvp.Value[2],
                                hour_3 = kvp.Value[3],
                                hour_4 = kvp.Value[4],
                                hour_5 = kvp.Value[5],
                                hour_6 = kvp.Value[6],
                                hour_7 = kvp.Value[7],
                                hour_8 = kvp.Value[8],
                                hour_9 = kvp.Value[9],
                                hour_10 = kvp.Value[10],
                                hour_11 = kvp.Value[11],
                                hour_12 = kvp.Value[12],
                                hour_13 = kvp.Value[13],
                                hour_14 = kvp.Value[14],
                                hour_15 = kvp.Value[15],
                                hour_16= kvp.Value[16],
                                hour_17 = kvp.Value[17],
                                hour_18 = kvp.Value[18],
                                hour_19 = kvp.Value[19],
                                hour_20 = kvp.Value[20],
                                hour_21 = kvp.Value[21],
                                hour_22 = kvp.Value[22],
                                hour_23 = kvp.Value[23]
                            });
                        }
                    }
                }
                finally
                {
                    connection.Clone();
                    connection.Dispose();
                }
            
        }
    }
}

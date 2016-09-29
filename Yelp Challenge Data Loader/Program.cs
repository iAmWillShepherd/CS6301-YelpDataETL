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
            LoadUsers(connection);

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

        private static void LoadUsers(MySqlConnection connection)
        {
            var userSql = 
                @"INSERT INTO user (
                    user_id,
                    name,
                    review_count,
                    average_stars,
                    yelping_since,
                    fans)
                VALUES (
                    @user_id,
                    @name,
                    @review_count,
                    @average_stars,
                    @yelping_since,
                    @fans);";

                var eliteStatusSql = 
                    @"INSERT INTO user_elite_status (
                        user_id,
                        year)
                    VALUES (
                        @user_id,
                        @year);";

                var votesSql = 
                    @"INSERT INTO user_vote (
                        user_id,
                        funny,
                        useful,
                        cool)
                    VALUES (
                        @user_id,
                        @funny,
                        @useful,
                        @cool);";

                var userFriendsSql = 
                    @"INSERT INTO user_friend (
                        user_id,
                        friend_user_id)
                    VALUES (
                        @user_id,
                        @friend_user_id);";

                var userComplimentsSql = 
                    @"INSERT INTO user_compliment (
                        user_id,
                        profile,
                        cute,
                        funny,
                        plain,
                        writer,
                        list,
                        note,
                        photos,
                        hot,
                        cool,
                        more)
                    VALUES (
                        @user_id,
                        @profile,
                        @cute,
                        @funny,
                        @plain,
                        @writer,
                        @list,
                        @note,
                        @photos,
                        @hot,
                        @cool,
                        @more);";

                var objs = File.ReadLines(GetFullFilename("user"))
                    .Select(x => JsonConvert.DeserializeObject(x))
                    .Select(x => {
                        dynamic obj = x;

                        var user = new {
                            obj.user_id,
                            name = obj.name,
                            obj.review_count,
                            obj.average_stars,
                            yelping_since = ((string)obj.yelping_since) + "-01",
                            obj.fans
                        };

                        var elite = new List<int>();
                        var friends = new List<string>();
                        var compliments = new Dictionary<string, int> {
                            { "profile", 0},
                            { "cute", 0},
                            { "funny", 0},
                            { "plain", 0},
                            { "writer", 0},
                            { "list", 0},
                            { "note", 0},
                            { "photos", 0},
                            { "hot", 0},
                            { "cool", 0},
                            { "more", 0},
                        };
                        var votes = new Dictionary<string, int>();
                        
                        foreach (var year in obj.elite)
                        {
                            elite.Add((int)year);
                        }

                        foreach (var friend in obj.friends)
                        {
                            friends.Add((string)friend.Value);
                        }

                        foreach (var kvp in obj.compliments)
                        {
                            if (compliments.ContainsKey(kvp.Name))
                                compliments[kvp.Name] = (int)kvp.Value.Value;
                        }

                        foreach (var kvp in obj.votes)
                        {
                            votes.Add(kvp.Name, (int)kvp.Value.Value);
                        }

                        return new {
                            user,
                            yearsElite = elite,
                            friends,
                            compliments,
                            votes
                        };
                    });

                connection.Open();
                var transaction = connection.BeginTransaction();

                try
                {
                    foreach (var obj in objs)
                    {
                        if (((string)obj.user.name).Contains("\xE9"))
                        {
                            break;
                        }

                        connection.Execute(userSql, obj.user);

                        connection.Execute(
                            userComplimentsSql,
                            new {
                                obj.user.user_id,
                                profile = obj.compliments["profile"],
                                cute = obj.compliments["cute"],
                                funny = obj.compliments["funny"],
                                plain = obj.compliments["plain"],
                                writer = obj.compliments["writer"],
                                list = obj.compliments["list"],
                                note = obj.compliments["note"],
                                photos = obj.compliments["photos"],
                                hot = obj.compliments["hot"],
                                cool = obj.compliments["cool"],
                                more = obj.compliments["more"]
                            }, transaction);

                        connection.Execute(
                            votesSql,
                            new {
                                obj.user.user_id,
                                funny = obj.votes["funny"],
                                useful = obj.votes["useful"],
                                cool = obj.votes["cool"]
                            }, transaction);

                        foreach (var year in obj.yearsElite)
                        {
                            connection.Execute(
                                eliteStatusSql, 
                                new {
                                    obj.user.user_id,
                                    year
                                }, transaction);
                        }

                        foreach (var friend in obj.friends)
                        {
                            connection.Execute(
                                userFriendsSql,
                                new {
                                    user_id = obj.user.user_id,
                                    friend_user_id = friend
                                }, transaction);
                        }
                    }

                    transaction.Commit();
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine(ex.Message);
                    transaction.Rollback();

                    throw;
                }
                finally
                {
                    transaction.Dispose();
                    connection.Close();
                    connection.Dispose();
                }
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

using System;
using System.Data;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Dapper;

namespace YelpDataETL.Loaders
{
    public class UserLoader
    {
        private static string _userSql =
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

        private static string _eliteStatusSql =
            @"INSERT INTO user_elite_status (
                user_id,
                year)
            VALUES (
                @user_id,
                @year);";

        private static string _votesSql =
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

        private static string _userFriendsSql =
            @"INSERT INTO user_friend (
                user_id,
                friend_user_id)
            VALUES (
                @user_id,
                @friend_user_id);";

        private static string _userComplimentsSql =
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

        public static void Load(IDbConnection connection)
        {
            Console.WriteLine("Loading users...");

            var objs = File.ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_user"))
                .Select(JsonConvert.DeserializeObject)
                .Select(x =>
                {
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

                    foreach (var year in obj.elite) elite.Add((int) year);

                    foreach (var friend in obj.friends) friends.Add((string) friend.Value);

                    foreach (var kvp in obj.compliments)
                    {
                        if (compliments.ContainsKey(kvp.Name))
                            compliments[kvp.Name] = (int)kvp.Value.Value;
                    }

                    foreach (var kvp in obj.votes) votes.Add(kvp.Name, (int) kvp.Value.Value);

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
                        break;

                    connection.Execute(_userSql, obj.user);

                    connection.Execute(
                        _userComplimentsSql,
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
                        _votesSql,
                        new
                        {
                            obj.user.user_id,
                            funny = obj.votes["funny"],
                            useful = obj.votes["useful"],
                            cool = obj.votes["cool"]
                        }, transaction);

                    foreach (int year in obj.yearsElite)
                    {
                        connection.Execute(
                            _eliteStatusSql,
                            new
                            {
                                obj.user.user_id,
                                year
                            }, transaction);
                    }

                    foreach (string friend in obj.friends)
                    {
                        connection.Execute(
                            _userFriendsSql,
                            new
                            {
                                user_id = obj.user.user_id,
                                friend_user_id = friend
                            }, transaction);
                    }
                }

                transaction.Commit();
            }
            catch (Exception)
            {
                transaction.Rollback();
                throw;
            }
            finally
            {
                transaction.Dispose();
                connection.Close();
                connection.Dispose();
            }

            Console.WriteLine("Completed loading users.");
        }
    }
}
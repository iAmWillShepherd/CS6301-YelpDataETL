using System.Data;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using Dapper;

namespace YelpDataLoader
{
    public class CheckinLoader
    {
        private static string _sql =
            @"INSERT INTO yelp.checkin (
            business_id,
            day_of_week,
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

        public static void Load(IDbConnection connection)
        {
            var objs = File.ReadLines(Helpers.GetFullFilename("yelp_academic_dataset_checkin"))
                .Select(x => JsonConvert.DeserializeObject(x))
                .Select(x =>
                {
                    dynamic obj = x;

                    var record = new
                    {
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
                        connection.Execute(_sql, new
                        {
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
                            hour_16 = kvp.Value[16],
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
                connection.Close();
                connection.Dispose();
            }
        }
    }
}
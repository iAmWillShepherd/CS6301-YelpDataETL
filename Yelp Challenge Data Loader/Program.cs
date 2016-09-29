using System;
using MySql.Data.MySqlClient;
using Dapper;
using System.IO;
using Newtonsoft.Json;
using System.Linq;

namespace ConsoleApplication
{
    public class Program
	{
		private static string basePath => Path.Combine(Directory.GetCurrentDirectory(), "data");
		private static MySqlConnection connection => new MySqlConnection("server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;");

		public static void Main(string[] args)
		{
			LoadReviews(connection);

			Console.WriteLine("Completed loading...");
		}

		private static string GetFullFilename(string x) => Path.Combine(basePath, x + ".json");

		private static void LoadReviews(MySqlConnection connection)
		{
			var sqlFormat = 
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
			try
			{
				connection.Open();

				var jsonObjs = File
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
							votes_funny = obj.vots?.funny,
							votes_useful = obj.votes?.useful,
							votes_cool = obj.votes?.cool
						};
					});

				foreach (var obj in jsonObjs) 
				{
					connection.Execute(sqlFormat, obj);
				}			
			}
			finally
			{
				connection.Close();
				connection.Dispose();
			}	
		}

		private static void LoadTips()
		{
			foreach (var line in File.ReadLines(GetFullFilename("business")))
			{

			}
		}

		private static void LoadBusinesses()
		{

		}

		private static void ImportUsers()
		{

		}
	}
}

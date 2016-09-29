using System;
using MySql.Data.MySqlClient;
using Dapper;
using System.Linq;
using System.IO;
using Newtonsoft;

namespace ConsoleApplication
{
    public class Program
	{
		private static string basePath => Path.Combine(Directory.GetCurrentDirectory(), "data");
		private static string connectionString => "server=localhost;user=root;password=P@ssword!;database=yelp;port=3306;";

		public static void Main(string[] args)
		{
			foreach (var file in Directory.GetFiles(basePath, "*.json"))
			{
				foreach (var line in File.ReadLines(file))
				{
					var json = Newtonsoft.Json.JsonConvert.DeserializeObject(line);
				}
			}
		}

		private static string GetFullFilename(string x) => Path.Combine(basePath, x + ".json");

		private static void ImportBusinesses()
		{
			foreach (var line in File.ReadLines(GetFullFilename("business")))
			{

			}
		}

		private static void ImportTips()
		{
			foreach (var line in File.ReadLines(GetFullFilename("business")))
			{

			}
		}

		private static void ImportReviews()
		{

		}

		private static void ImportUsers()
		{

		}
	}
}

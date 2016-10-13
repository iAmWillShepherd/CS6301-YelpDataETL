using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using YelpDataETL.Loaders;

namespace YelpDataETL
{
    public class Program
    {    
        public static void Main(string[] args)
        {
            var loaders = new List<Task> {
                Task.Run(() => BusinessLoader.Load(Helpers.CreateConnection())),
                Task.Run(() => CheckinLoader.Load(Helpers.CreateConnection())),
                Task.Run(() => ReviewLoader.Load(Helpers.CreateConnection())),
                Task.Run(() => TipLoader.Load(Helpers.CreateConnection())),
                Task.Run(() => UserLoader.Load(Helpers.CreateConnection()))
            };

            Task.WaitAll(loaders.ToArray());

            Console.WriteLine("Done.");
        }
    }
}
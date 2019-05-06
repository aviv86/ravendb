using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using SlowTests.Cluster;
using SlowTests.Issues;
using Sparrow;
using StressTests.Server.Replication;
using Xunit.Sdk;

namespace Tryouts
{
    public class TestItem
    {
        public long A, B;
    }

    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "Test"
            }.Initialize();

            var list = new List<(DateTime, double)>();

            DateTime start = default;

            var lines = File.ReadLines(@"C:\Users\ayende\source\repos\ConsoleApp20\ConsoleApp20\bin\Debug\netcoreapp2.2\out.csv");
            foreach (var line in lines)
            {
                var parts = line.Split(',');
                if (parts.Length != 2)
                    continue;
                if (DateTime.TryParseExact(parts[0], "o", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var date) == false)
                    continue;
                if (double.TryParse(parts[1], out var d) == false)
                    continue;

                list.Add((date, d));
                if(list.Count >= 250)
                {
                    FlushBpm(store, list);
                    if ((date - start).TotalDays > 30)
                    {
                        Console.WriteLine(date);
                        start = date;
                    }
                }
            }

            FlushBpm(store, list);
        }

        private static void FlushBpm(IDocumentStore store, List<(DateTime, double)> list)
        {
            using (var s = store.OpenSession())
            {
                var ts = s.TimeSeriesFor("users/ayende");

                foreach (var item in list)
                {
                    ts.Append("BPM", item.Item1, "watches/fitbit", new[] { item.Item2 });
                }

                s.SaveChanges();
            }
            list.Clear();
        }

        private static async Task WriteMillionDocs(DocumentStore store)
        {
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < 1_000_000; i++)
                {
                    var t = bulk.StoreAsync(new TestItem
                    {
                        A = i,
                        B = i
                    });
                    if (t.IsCompleted)
                        continue;
                    await t;
                }
            }
        }
    }
}

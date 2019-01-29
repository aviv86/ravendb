using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using FastTests.Graph;
using Lucene.Net.Util;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit.Sdk;
using Constants = Raven.Client.Constants;

namespace Tryouts
{
   
    public static class Program
    {

        public class Tracking
        {
            public string Id { get; set; }
        }

        public static void Main(string[] args)
        {
            var ids = new Dictionary<string, List<string>>();

            using (var store = new DocumentStore
            {
                Urls = new []{"http://localhost:8080"},
                Database = "counters"
            }.Initialize())
            {
                var sp1 = Stopwatch.StartNew();
                var maxPerDoc = 0;

                Console.WriteLine("started query...");

                using (var session = store.OpenSession())
                {
                    var take = 3_000;
                    var numberOfDocs = session.Query<Tracking>().Count();
                    var query = session.Query<Tracking>()
                        .Skip(numberOfDocs - take)
                        .Take(take)
                        .Select(t => new
                        {
                            Counters = RavenQuery.Raw<List<string>>(@"t[""@metadata""][""@counters""]")
                        });

                    var stream = session.Advanced.Stream(query);

                    while (stream.MoveNext())
                    {
                        if (stream.Current.Document.Counters.Count < 100)
                            continue;

                        ids.Add(stream.Current.Id, stream.Current.Document.Counters);
                        maxPerDoc = Math.Max(maxPerDoc, stream.Current.Document.Counters.Count);
                    }
                }

                Console.WriteLine($"finished query in : {sp1.Elapsed}");
                Console.WriteLine($"max counters per doc : {maxPerDoc}");
                Console.WriteLine($"ids.Count = {ids.Count}");

                var sp = Stopwatch.StartNew();
                var count = 0;
                var docCount = 0;

                Console.WriteLine("starting to increment counters...");
                
                foreach (var kvp in ids)
                {
                    using (var session = store.OpenSession())
                    {
                        var cf = session.CountersFor(kvp.Key);

                        foreach (var counter in kvp.Value)
                        {
                            cf.Increment(counter);
                        }

                        count += kvp.Value?.Count ?? 0;
                        session.SaveChanges();
                    }

                    if (++docCount % 500 == 0)
                        Console.WriteLine($"incremented {count} counters");

                }
                
                Console.WriteLine("finished incrementing counters.");
                Console.WriteLine($"total counters count : {count}.");
                Console.WriteLine($"document count : {docCount}");
                Console.WriteLine($"time: { sp.Elapsed}.");
            }

        }
    }
}

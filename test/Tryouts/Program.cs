using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Counters;

namespace Tryouts
{
    public class Program
    {
        private class User
        {

        }

        public static void Main(string[] args)
        {
            using (var store = new DocumentStore()
            {
                Database = "test",
                Urls = new[] { "http://localhost:8080" }
            }.Initialize())
            {
                var sp = Stopwatch.StartNew();
                var ids = InsertDocs(store);
                IncrementCounters(store, ids, "incrementing");

/*                var ids = new List<string>();
                for (int i = 1; i <= 10000; i++)
                {
                    ids.Add("users/"+i);
                }

                IncrementCounters(store, ids, "updating");*/
                Console.WriteLine($"total time: {sp.Elapsed}ms");
                Console.ReadKey();
            }
        }

        private static void IncrementCounters(IDocumentStore store, List<string> ids, string op)
        {
            var documentCountersOperations = new List<DocumentCountersOperation>();
            var rand = new Random();
            var count = 0;
            var sp = Stopwatch.StartNew();
            Console.WriteLine($"started {op} counters");
            var numOfCountersPerDoc = 20;

            foreach (var id in ids)
            {
                for (int j = 0; j < numOfCountersPerDoc; j++)
                {
                    var counterOperations = new List<CounterOperation>();

                    var counterOp = new CounterOperation
                    {
                        CounterName = "likes/" + j,
                        Type = CounterOperationType.Increment,
                        Delta = rand.Next(1, 1000)
                    };

                    counterOperations.Add(counterOp);

                    documentCountersOperations.Add(new DocumentCountersOperation
                    {
                        DocumentId = id,
                        Operations = counterOperations
                    });
                }

                count+= numOfCountersPerDoc;

                if (count % 10000 != 0)
                    continue;

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = documentCountersOperations
                }));

                documentCountersOperations.Clear();

                Console.WriteLine($"proccessed {count} counters");
            }

            if (documentCountersOperations.Count > 0)
            {
                Console.WriteLine("#bug");
            }

            Console.WriteLine($"finished {op} counters in {sp.Elapsed}ms");
        }

        private static List<string> InsertDocs(IDocumentStore store)
        {
            Console.WriteLine("started inserting documents");

            var ids = new List<string>();
            var count = 1;

            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 100; i++)
                {
                    for (int j = 0; j < 1000; j++)
                    {
                        var id = "users/" + count++;
                        bulkInsert.Store(new User(), id);
                        ids.Add(id);
                    }
                }
            }

            Console.WriteLine($"finished inserting {count} documents");

            return ids;

        }
    }
}

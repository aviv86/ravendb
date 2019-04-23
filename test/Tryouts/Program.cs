using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic;
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
            using (var store = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "test"
            })
            {
                store.Initialize();

                var sp = Stopwatch.StartNew();
                var tasks = new Task[10];
                for (int i = 0; i < 10; i++)
                {
                    tasks[i] = WriteMillionDocs(store);
                }
                Task.WaitAll(tasks);
                Console.WriteLine(sp.ElapsedMilliseconds);
            }
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

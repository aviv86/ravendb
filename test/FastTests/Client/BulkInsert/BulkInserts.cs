﻿using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.BulkInsert;
using Xunit;

namespace FastTests.Client.BulkInsert
{
    public class BulkInserts : RavenTestBase
    {
        [Fact]
        public void SimpleBulkInsertShouldWork()
        {
            var fooBars = new[]
            {
                new FooBar { Name = "John Doe" },
                new FooBar { Name = "Jane Doe" },
                new FooBar { Name = "Mega John" },
                new FooBar { Name = "Mega Jane" }
            };

            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(fooBars[0]);
                    bulkInsert.Store(fooBars[1]);
                    bulkInsert.Store(fooBars[2]);
                    bulkInsert.Store(fooBars[3]);
                }

                using (var session = store.OpenSession())
                {
                    var doc1 = session.Load<FooBar>("FooBars/1");
                    var doc2 = session.Load<FooBar>("FooBars/2");
                    var doc3 = session.Load<FooBar>("FooBars/3");
                    var doc4 = session.Load<FooBar>("FooBars/4");

                    Assert.NotNull(doc1);
                    Assert.NotNull(doc2);
                    Assert.NotNull(doc3);
                    Assert.NotNull(doc4);

                    Assert.Equal("John Doe", doc1.Name);
                    Assert.Equal("Jane Doe", doc2.Name);
                    Assert.Equal("Mega John", doc3.Name);
                    Assert.Equal("Mega Jane", doc4.Name);
                }
            }
        }

        [Fact]
        public async Task AsyncSimpleBulkInsertShouldWork()
        {
            var fooBars = new[]
            {
                new FooBar { Name = "John Doe" },
                new FooBar { Name = "Jane Doe" },
                new FooBar { Name = "Mega John" },
                new FooBar { Name = "Mega Jane" }
            };

            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    await bulkInsert.StoreAsync(fooBars[0]);
                    await bulkInsert.StoreAsync(fooBars[1]);
                    await bulkInsert.StoreAsync(fooBars[2]);
                    await bulkInsert.StoreAsync(fooBars[3]);
                }

                using (var session = store.OpenSession())
                {
                    var doc1 = session.Load<FooBar>("FooBars/1");
                    var doc2 = session.Load<FooBar>("FooBars/2");
                    var doc3 = session.Load<FooBar>("FooBars/3");
                    var doc4 = session.Load<FooBar>("FooBars/4");

                    Assert.NotNull(doc1);
                    Assert.NotNull(doc2);
                    Assert.NotNull(doc3);
                    Assert.NotNull(doc4);

                    Assert.Equal("John Doe", doc1.Name);
                    Assert.Equal("Jane Doe", doc2.Name);
                    Assert.Equal("Mega John", doc3.Name);
                    Assert.Equal("Mega Jane", doc4.Name);
                }
            }
        }

        [Fact]
        public async Task KilledTooEarly()
        {
            await Assert.ThrowsAsync<BulkInsertAbortedException>(async () =>
            {
                using (var store = GetDocumentStore())
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        await bulkInsert.StoreAsync(new FooBar());
                        await bulkInsert.AsyncKill();
                    }
                }
            });
        }

        private class FooBar : IEquatable<FooBar>
        {
            public string Name { get; set; }

            public bool Equals(FooBar other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(Name, other.Name);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((FooBar)obj);
            }

            public override int GetHashCode()
            {
                return Name?.GetHashCode() ?? 0;
            }

            public static bool operator ==(FooBar left, FooBar right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(FooBar left, FooBar right)
            {
                return !Equals(left, right);
            }
        }
    }
}

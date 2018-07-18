using System;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11539 : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask<Company>
        {
            public Index1()
            {
                Map = companies =>
                    from company in companies
                    select new
                    {
                        Name = company.Name
                    };
            }
        }

        private class Index2 : AbstractIndexCreationTask<Order>
        {
            public Index2()
            {
                Map = orders =>
                    from order in orders
                    select new
                    {
                        Company = order.Company,
                        OrderedAt_Year = order.OrderedAt.Year
                    };
            }
        }

        [Fact]
        public void QueryWithLoadButWithoutJsProjectionShouldNotUseAliasNotation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");

                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                let c = RavenQuery.Load<Company>(o.Company)
                                select c.Name;

                    Assert.Equal("from Orders load Company as c " +
                                 "select c.Name", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("HR", result[0]);
                }
            }
        }

        [Fact]
        public void QueryWithStaticIndexAndJsProjectionShouldGenerateCorrectPaths()
        {
            using (var store = GetDocumentStore())
            {
                new Index2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderedAt = new DateTime(2018, 1, 1),
                        Company = "companies/1-A"
                    });
                    session.Store(new Company
                    {
                        Name = "HR",
                        Address1 = "Caesarea, Israel"
                    }, "companies/1-A");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from i in session.Query<Order, Index2>()
                                let c = RavenQuery.Load<Company>(i.Company)
                                let format = (Func<Company, string>)(x => x.Name + ", " + x.Address1)
                                select new
                                {
                                    i.OrderedAt, Company = format(c)
                                };

                    Assert.Equal(
@"declare function output(i, c) {
	var format = function(x){return x.Name+"", ""+x.Address1;};
	return { OrderedAt : new Date(Date.parse(i.OrderedAt)), Company : format(c) };
}
from index 'Index2' as i load i.Company as c select output(i, c)" , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(new DateTime(2018, 1, 1), result[0].OrderedAt);
                    Assert.Equal("HR, Caesarea, Israel", result[0].Company);

                }
            }
        }

        [Fact]
        public void QuerySelcetNestedMemberWithStaticIndexShouldGenerateCorrectPaths()
        {
            using (var store = GetDocumentStore())
            {
                new Index2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderedAt = new DateTime(2018, 1, 1)
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order, Index2>()
                        .Where(i => i.OrderedAt.Year > 1997);

                    Assert.Equal("from index 'Index2' where OrderedAt_Year > $p0"
                            , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(new DateTime(2018, 1, 1), result[0].OrderedAt);

                }
            }
        }

        [Fact]
        public void QueryWithStaticIndexAndLoadShouldGenerateCorrectSelectPath()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderedAt = new DateTime(2018, 1, 1)
                    }, "orders/1-A");
                    session.Store(new Company
                    {
                        Name = "HR"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = from o in session.Query<Order>()
                            let c = RavenQuery.Load<Company>(o.Company)
                            select new
                            {
                                c.Name,
                                o.Employee
                            };

                    var y = q.ToString();

                    var query = from c in session.Query<Order, Index2>()
                                let o = RavenQuery.Load<Order>("orders/1")
                                select o.OrderedAt;

                    Assert.Equal("from index 'Index1' load $p0 as o " +
                                 "select o.OrderedAt" , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(new DateTime(2018, 1, 1), result[0]);

                }
            }
        }
    }
}


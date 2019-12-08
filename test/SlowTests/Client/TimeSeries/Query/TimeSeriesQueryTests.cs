﻿using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesQueryTests : RavenTestBase
    {
        public TimeSeriesQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        public class TimeSeriesRangeAggregation
        {
#pragma warning disable 649
            public long Count;
            public double? Max, Min, Last, First, Avg;
            public DateTime To, From;
#pragma warning restore 649
        }

        public class TimeSeriesAggregation
        {
            public long Count { get; set; }
            public TimeSeriesRangeAggregation[] Results { get; set; }
        }

        private class PeopleIndex : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex()
            {
                Map = people => from person in people
                                select new
                                {
                                    person.Age
                                };
            }

            public override string IndexName => "People";
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from u in users
                               select new
                                {
                                    u.Age
                                };
            }
        }

        private class Person
        {
            public string Name { get; set; }

            public int Age { get; set; }

            public string WorksAt { get; set; }

            public string Event { get; set;  }

        }

        private class Event
        {
            public DateTime Start { get; set; }

            public DateTime End { get; set; }

            public string Description { get; set; }

        }

        private class Watch
        {
            public string Manufacturer { get; set;  }

            public double Accuracy { get; set;  }
        }


        public class RawQueryResult
        {
            public TimeSeriesAggregation HeartRate { get; set; }

            public TimeSeriesAggregation BloodPressure { get; set; }

            public TimeSeriesAggregation Stocks { get; set; }

            public string Name { get; set; }
        }

        private class TimeSeriesRaw
        {
            public long Count { get; set; }
            public TimeSeriesValue[] Results { get; set; }
        }

        [Fact]
        public unsafe void CanQueryTimeSeriesAggregation_DeclareSyntax_AllDocsQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from @all_docs as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var agg = query.First();

                    if (agg.Count != 3)
                    {
                        var db = GetDocumentDatabaseInstanceFor(store).Result;
                        var tss = db.DocumentsStorage.TimeSeriesStorage;
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var reader = tss.GetReader(ctx, "users/ayende", "Heartrate", baseline, baseline.AddDays(1));

                            Assert.True(reader.Init());

                            Assert.NotNull(reader._tvr);

                            var key = reader._tvr.Read(0, out var size);

                            TimeSeriesValuesSegment.ParseTimeSeriesKey(key, size, out var docId, out var name, out DateTime baseline2);

                            Assert.Equal("users/ayende", docId);
                            Assert.Equal("Heartrate", name);
                            Assert.Equal(baseline.AddMinutes(61), baseline2);

                            Assert.Equal(1, reader.SegmentsOrValues().Count());

                            Assert.False(query.First().Count == 3, "Query assertion failed once and passed on second try. sanity check passed");

                            //Assert.True(false, "Query assertion failed twice. sanity check passed");
                        }
                    }

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First);
                    Assert.Equal(59, val.Min);

                    Assert.Equal(69, val.Last);
                    Assert.Equal(79, val.Max);

                    Assert.Equal(baseline.AddMinutes(60), val.From);
                    Assert.Equal(baseline.AddMinutes(120), val.To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from Users as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First);
                    Assert.Equal(59, val.Min);

                    Assert.Equal(69, val.Last);
                    Assert.Equal(79, val.Max);

                    Assert.Equal(baseline.AddMinutes(60), val.From);
                    Assert.Equal(baseline.AddMinutes(120), val.To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren", Age = 50}, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from Users as u
    where u.Age > 49
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First);
                    Assert.Equal(59, val.Min);

                    Assert.Equal(69, val.Last);
                    Assert.Equal(79, val.Max);

                    Assert.Equal(baseline.AddMinutes(60), val.From);
                    Assert.Equal(baseline.AddMinutes(120), val.To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren", Age = 50 }, "users/ayende");
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });

                    session.TimeSeriesFor("users/ayende")
                        .Append("Heartrate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    session.SaveChanges();
                }

                new UsersIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from index 'UsersIndex' as u
    where u.Age > 49
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First);
                    Assert.Equal(59, val.Min);

                    Assert.Equal(69, val.Last);
                    Assert.Equal(79, val.Max);

                    Assert.Equal(baseline.AddMinutes(60), val.From);
                    Assert.Equal(baseline.AddMinutes(120), val.To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_WithOtherFields()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
declare timeseries out(p) 
{
    from p.HeartRate between $start and $end 
    group by 1h 
    select min(), max()
}
from index 'People' as p 
where p.Age > 49
select out(p) as HeartRate, p.Name 
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal("Oren", agg.Name);

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_MultipleSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                var baseline2 = DateTime.Today.AddDays(-1);


                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("BloodPressure", baseline2.AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("BloodPressure", baseline2.AddMinutes(62), "watches/apple", new[] { 179d });
                        tsf.Append("BloodPressure", baseline2.AddMinutes(63), "watches/apple", new[] { 168d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
declare timeseries heart_rate(doc) 
{
    from doc.HeartRate between $start and $end
    group by 1h 
    select min(), max()
}
declare timeseries blood_pressure(doc) 
{
    from doc.BloodPressure between $start2 and $end2 
    group by 1h 
    select min(), max(), avg()
}
from index 'People' as p 
where p.Age > 49
select heart_rate(p) as HeartRate, blood_pressure(p) as BloodPressure
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1))
                        .AddParameter("start2", baseline2)
                        .AddParameter("end2", baseline2.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);

                        var bloodPressure = agg.BloodPressure;

                        Assert.Equal(3, bloodPressure.Count);

                        Assert.Equal(1, bloodPressure.Results.Length);

                        val = bloodPressure.Results[0];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);

                        double expectedAvg = (159 + 168 + 179) / 3.0;

                        Assert.Equal(expectedAvg, val.Avg);

                        Assert.Equal(baseline2.AddMinutes(60), val.From);
                        Assert.Equal(baseline2.AddMinutes(120), val.To);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_FromLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        var company = $"companies/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = company
                        }, id);

                        session.Store(new Company(), company);

                        var tsf = session.TimeSeriesFor(company);

                        tsf.Append("Stocks", baseline.AddMinutes(61), "tag", new[] { 1259.51d });
                        tsf.Append("Stocks", baseline.AddMinutes(62), "tag", new[] { 1279.62d });
                        tsf.Append("Stocks", baseline.AddMinutes(63), "tag", new[] { 1269.73d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(c) 
{
    from c.Stocks between $start and $end
    group by 1h 
    select min(), max(), avg()
}
from People as p
where p.Age > 49
load p.WorksAt as Company
select out(Company)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1259.51d, val.Min);
                        Assert.Equal(1279.62d, val.Max);

                        double expectedAvg = (1259.51d + 1279.62d + 1269.73d) / 3.0;
                        Assert.Equal(expectedAvg, val.Avg);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
from People as p 
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(3, result.Count);

                    for (int i = 0; i < 3; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
from People as p 
where p.Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_AsAlias()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
as HeartRate
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];
                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_WithOtherFields()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
as HeartRate, Name 
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal("Oren", agg.Name);

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_MultipleSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                var baseline2 = DateTime.Today.AddDays(-1);


                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("BloodPressure", baseline2.AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("BloodPressure", baseline2.AddMinutes(62), "watches/apple", new[] { 179d });
                        tsf.Append("BloodPressure", baseline2.AddMinutes(63), "watches/apple", new[] { 168d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end
    group by 1h 
    select min(), max())
as HeartRate, timeseries(
    from BloodPressure between $start2 and $end2 
    group by 1h 
    select min(), max(), avg())
as BloodPressure
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1))
                        .AddParameter("start2", baseline2)
                        .AddParameter("end2", baseline2.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);

                        var bloodPressure = agg.BloodPressure;

                        Assert.Equal(3, bloodPressure.Count);

                        Assert.Equal(1, bloodPressure.Results.Length);

                        val = bloodPressure.Results[0];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);

                        double expectedAvg = (159 + 168 + 179) / 3.0;

                        Assert.Equal(expectedAvg, val.Avg);

                        Assert.Equal(baseline2.AddMinutes(60), val.From);
                        Assert.Equal(baseline2.AddMinutes(120), val.To);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_AliasNotation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from People as p
where p.Age > 49
select timeseries(
    from p.HeartRate between $start and $end
    group by 1h 
    select min(), max())
as HeartRate
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_FromLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        var company = $"companies/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = company
                        }, id);

                        session.Store(new Company(), company);

                        var tsf = session.TimeSeriesFor(company);

                        tsf.Append("Stocks", baseline.AddMinutes(61), "tag", new[] { 1259.51d });
                        tsf.Append("Stocks", baseline.AddMinutes(62), "tag", new[] { 1279.62d });
                        tsf.Append("Stocks", baseline.AddMinutes(63), "tag", new[] { 1269.73d });
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from People as p
where p.Age > 49
load p.WorksAt as Company
select timeseries(
    from Company.Stocks between $start and $end
    group by 1h 
    select min(), max(), avg())
as Stocks
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var stocks = agg.Stocks;

                        Assert.Equal(3, stocks.Count);

                        Assert.Equal(1, stocks.Results.Length);

                        var val = stocks.Results[0];

                        Assert.Equal(1259.51d, val.Min);
                        Assert.Equal(1279.62d, val.Max);

                        double expectedAvg = (1259.51d + 1279.62d + 1269.73d) / 3.0;
                        Assert.Equal(expectedAvg, val.Avg);

                        Assert.Equal(baseline.AddMinutes(60), val.From);
                        Assert.Equal(baseline.AddMinutes(120), val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_GroupByMonth()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_NoBetween()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate
    group by '1 month' 
    select min(), max()
}
from People as doc
where doc.Age > 49
select out(doc)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/fitbit", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/apple", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/fitbit", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/apple", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRaw>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);


                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(59, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp);
                        
                        val = agg.Results[4];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp);

                        val = agg.Results[5];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_MultipleParameters()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {

                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            Event = "events/" + i
                        }, id);

                        session.Store(new Event
                        {
                            Start = baseline.AddDays(i - 1),
                            End = baseline.AddDays(i - 1).AddMonths(2)
                        }, "events/" + i);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });


                        tsf.Append("HeartRate", baseline.AddDays(1).AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddDays(1).AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddDays(1).AddMinutes(63), "watches/fitbit", new[] { 69d });


                        tsf.Append("HeartRate", baseline.AddDays(5).AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddDays(5).AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddDays(5).AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(p, e) 
{
    from p.HeartRate between $start and $end
    group by '1 month' 
    select min(), max()
}
from People as doc
where doc.Age > 49
load doc.Event as e
select out(doc, e)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTag_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRaw>(@"
declare timeseries out(x) 
{
    from x.HeartRate
        where Tag == 'watches/fitbit'
}
from People as doc
where doc.Age > 49
select out(doc)
");
                    
                    var result = query.ToList();
              
                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        //Assert.Equal(6, agg.Count);

                        Assert.Equal(4, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(59, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnValue_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRaw>(@"
declare timeseries out(x) 
{
    from x.HeartRate
        where Values[0] > 70
}
from People as doc
where doc.Age > 49
select out(doc)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTagAndValue_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRaw>(@"
from People as doc
where doc.Age > 49
select timeseries(from doc.HeartRate where Tag == 'watches/fitbit' and Values[0] > 70)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        //Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Tag == 'watches/fitbit'
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(69, val.Max);
                        Assert.Equal(64, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min);
                        Assert.Equal(179, val.Max);
                        Assert.Equal(174, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnValue()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] > 70
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min);
                        Assert.Equal(79, val.Max);
                        Assert.Equal(79, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);
                        Assert.Equal(169, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnQueryParameter()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] > $val
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2))
                        .AddParameter("val", 70);

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min);
                        Assert.Equal(79, val.Max);
                        Assert.Equal(79, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);
                        Assert.Equal(169, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnFunctionArgument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/fitbit", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x, val) 
{
    from x.HeartRate between $start and $end
        where Values[0] > val
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.WorksAt as c
select out(doc, c.AccountsReceivable)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min);
                        Assert.Equal(79, val.Max);
                        Assert.Equal(79, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(179, val.Max);
                        Assert.Equal(169, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereIn()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/sony", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Tag in ('watches/fitbit', 'watches/apple')
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(5, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);
                        Assert.Equal(69, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(169, val.Max);
                        Assert.Equal(164, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8
                    }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/apple", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src    
        where src.Accuracy > 2
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(69, val.Max);
                        Assert.Equal(64, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min);
                        Assert.Equal(169, val.Max);
                        Assert.Equal(169, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereBetween()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id);

                        tsf.Append("HeartRate", baseline.AddMinutes(61), "watches/fitbit", new[] { 59d });
                        tsf.Append("HeartRate", baseline.AddMinutes(62), "watches/apple", new[] { 79d });
                        tsf.Append("HeartRate", baseline.AddMinutes(63), "watches/fitbit", new[] { 69d });

                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(61), "watches/apple", new[] { 159d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(62), "watches/sony", new[] { 179d });
                        tsf.Append("HeartRate", baseline.AddMonths(1).AddMinutes(63), "watches/fitbit", new[] { 169d });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregation>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] between 70 and 160
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(2));

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(5, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min);
                        Assert.Equal(79, val.Max);
                        Assert.Equal(69, val.Avg);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min);
                        Assert.Equal(169, val.Max);
                        Assert.Equal(164, val.Avg);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

    }
}

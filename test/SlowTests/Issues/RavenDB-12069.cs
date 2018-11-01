using System;
using FastTests;
using Newtonsoft.Json;
using Sparrow.Extensions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12069 : RavenTestBase
    {
        private class Flight
        {
            public string Destination { get; set; }
            public DateTime DepartureDate { get; set; }
            public DateTime DepartureDateUtc { get; set; }
            public DateTime DepartureDateLocal { get; set; }
            public TimeSpan Duration { get; set; }
        }

        [Fact]
        public void GetDefaultRavenFormatShouldHandleLocalKind()
        {
            var localDt = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local);
            var unspecifiedDt = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified);

            Assert.NotEqual(localDt.GetDefaultRavenFormat(), unspecifiedDt.GetDefaultRavenFormat());
            Assert.Equal(localDt.ToString("o"), localDt.GetDefaultRavenFormat());
            Assert.Equal(unspecifiedDt.ToString("o"), unspecifiedDt.GetDefaultRavenFormat());
        }

        [Fact]
        public void JsonSerializerWithDateTimeZoneHandlingRoundtripKind()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.CustomizeJsonSerializer +=
                    serializer => serializer.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind
            }))
            {
                using (var session = store.OpenSession())
                {
                    var flight = new Flight
                    {
                        Destination = "Dubai",
                        DepartureDate = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified),
                        DepartureDateUtc = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Utc),
                        DepartureDateLocal = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local),
                        Duration = TimeSpan.FromHours(5.5)
                    };

                    session.Store(flight, "flights/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var flight = session.Load<Flight>("flights/1");
                    Assert.Equal(DateTimeKind.Unspecified, flight.DepartureDate.Kind);
                    Assert.Equal(DateTimeKind.Utc, flight.DepartureDateUtc.Kind);
                    Assert.Equal(DateTimeKind.Local, flight.DepartureDateLocal.Kind);

                    var date = new DateTime(2013, 1, 21, 0, 0, 0);
                    Assert.Equal(date.ToString("o"), flight.DepartureDate.ToString("o"));
                    Assert.Equal(DateTime.SpecifyKind(date, DateTimeKind.Utc).ToString("o"),
                        flight.DepartureDateUtc.ToString("o"));
                    Assert.Equal(DateTime.SpecifyKind(date, DateTimeKind.Local).ToString("o"),
                        flight.DepartureDateLocal.ToString("o"));
                }
            }


            // default behavior should give the same results

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var flight = new Flight
                    {
                        Destination = "Dubai",
                        DepartureDate = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified),
                        DepartureDateUtc = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Utc),
                        DepartureDateLocal = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local),
                        Duration = TimeSpan.FromHours(5.5)
                    };

                    session.Store(flight, "flights/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var flight = session.Load<Flight>("flights/1");
                    Assert.Equal(DateTimeKind.Unspecified, flight.DepartureDate.Kind);
                    Assert.Equal(DateTimeKind.Utc, flight.DepartureDateUtc.Kind);
                    Assert.Equal(DateTimeKind.Local, flight.DepartureDateLocal.Kind);

                    var date = new DateTime(2013, 1, 21, 0, 0, 0);
                    Assert.Equal(date.ToString("o"), flight.DepartureDate.ToString("o"));
                    Assert.Equal(DateTime.SpecifyKind(date, DateTimeKind.Utc).ToString("o"),
                        flight.DepartureDateUtc.ToString("o"));
                    Assert.Equal(DateTime.SpecifyKind(date, DateTimeKind.Local).ToString("o"),
                        flight.DepartureDateLocal.ToString("o"));
                }
            }
        }

        [Fact]
        public void JsonSerializerWithDateTimeZoneHandlingLocal()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.CustomizeJsonSerializer +=
                    serializer => serializer.DateTimeZoneHandling = DateTimeZoneHandling.Local
            }))
            {
                using (var session = store.OpenSession())
                {
                    var flight = new Flight
                    {
                        Destination = "Dubai",
                        DepartureDate = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified),
                        DepartureDateUtc = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Utc),
                        DepartureDateLocal = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local),
                        Duration = TimeSpan.FromHours(5.5)
                    };

                    session.Store(flight, "flights/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var flight = session.Load<Flight>("flights/1");
                    Assert.Equal(DateTimeKind.Local, flight.DepartureDate.Kind);
                    Assert.Equal(DateTimeKind.Local, flight.DepartureDateUtc.Kind);
                    Assert.Equal(DateTimeKind.Local, flight.DepartureDateLocal.Kind);

                    var date = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local);
                    Assert.Equal(date.ToString("o"), flight.DepartureDate.ToString("o"));
                    Assert.Equal(date.ToString("o"), flight.DepartureDateUtc.ToString("o"));
                    Assert.Equal(date.ToString("o"), flight.DepartureDateLocal.ToString("o"));
                }
            }
        }




        [Fact]
        public void JsonSerializerWithDateTimeZoneHandlingUtc()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.CustomizeJsonSerializer +=
                    serializer => serializer.DateTimeZoneHandling = DateTimeZoneHandling.Utc
            }))
            {

                using (var session = store.OpenSession())
                {
                    var flight = new Flight
                    {
                        Destination = "Dubai",
                        DepartureDate = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified),
                        DepartureDateUtc = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Utc),
                        DepartureDateLocal = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local),
                        Duration = TimeSpan.FromHours(5.5)
                    };

                    session.Store(flight, "flights/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var flight = session.Load<Flight>("flights/1");
                    Assert.Equal(DateTimeKind.Utc, flight.DepartureDate.Kind);
                    Assert.Equal(DateTimeKind.Utc, flight.DepartureDateUtc.Kind);
                    Assert.Equal(DateTimeKind.Utc, flight.DepartureDateLocal.Kind);

                    var date = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Utc);
                    Assert.Equal(date.ToString("o"), flight.DepartureDate.ToString("o"));
                    Assert.Equal(date.ToString("o"), flight.DepartureDateUtc.ToString("o"));
                    Assert.Equal(date.ToString("o"), flight.DepartureDateLocal.ToString("o"));
                }
            }
        }

        [Fact]
        public void JsonSerializerWithDateTimeZoneHandlingUnspecified()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.CustomizeJsonSerializer +=
                    serializer => serializer.DateTimeZoneHandling = DateTimeZoneHandling.Unspecified
            }))
            {

                using (var session = store.OpenSession())
                {
                    var flight = new Flight
                    {
                        Destination = "Dubai",
                        DepartureDate = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified),
                        DepartureDateUtc = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Utc),
                        DepartureDateLocal = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Local),
                        Duration = TimeSpan.FromHours(5.5)
                    };

                    session.Store(flight, "flights/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var flight = session.Load<Flight>("flights/1");
                    Assert.Equal(DateTimeKind.Unspecified, flight.DepartureDate.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, flight.DepartureDateUtc.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, flight.DepartureDateLocal.Kind);

                    var date = new DateTime(2013, 1, 21, 0, 0, 0, DateTimeKind.Unspecified);
                    Assert.Equal(date.ToString("o"), flight.DepartureDate.ToString("o"));
                    Assert.Equal(date.ToString("o"), flight.DepartureDateUtc.ToString("o"));
                    Assert.Equal(date.ToString("o"), flight.DepartureDateLocal.ToString("o"));
                }
            }
        }
    }
}

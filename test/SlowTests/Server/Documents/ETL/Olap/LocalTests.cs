﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class LocalTests : EtlTestBase
    {
        internal const string DefaultFrequency = "* * * * *"; // every minute

        public LocalTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SimpleTransformation()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 31; i++)
                        {
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                ShipVia = $"shippers/{i}",
                                Company = $"companies/{i}"
                            });
                        }

                        for (int i = 0; i < 28; i++)
                        {
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i + 31}",
                                OrderedAt = baseline.AddMonths(1).AddDays(i),
                                ShipVia = $"shippers/{i + 31}",
                                Company = $"companies/{i + 31}"
                            });
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";
                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);

                    var expectedFields = new[] { "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    foreach (var fileName in files)
                    {
                        using (var fs = File.OpenRead(fileName))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));
                                var data = rowGroupReader.ReadColumn((DataField)field).Data;

                                Assert.True(data.Length == 31 || data.Length == 28);
                                var count = data.Length == 31 ? 0 : 31;

                                if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                    continue;

                                foreach (var val in data)
                                {
                                    var expected = field.Name switch
                                    {
                                        ParquetTransformedItems.DefaultIdColumn => $"orders/{count}",
                                        "Company" => $"companies/{count}",
                                        "ShipVia" => $"shippers/{count}",
                                        _ => null
                                    };

                                    Assert.Equal(expected, val);
                                    count++;
                                }

                            }

                        }

                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task SimpleTransformation2()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                continue;
                            
                            long count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case ParquetTransformedItems.DefaultIdColumn:
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "RequireAt":
                                        var expected = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count).AddDays(7), DateTimeKind.Utc));
                                        Assert.Equal(expected, val);
                                        break;
                                    case "Total":
                                        var expectedTotal = count * count * 10;
                                        Assert.Equal(expectedTotal, val);
                                        break;
                                }

                                count++;

                            }
                        }
                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task SimpleTransformation_PartitionByDay()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < TimeSpan.FromDays(7).TotalHours; i++)
                        {
                            var orderedAt = baseline.AddHours(i);
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var day = orderDate.getDay();
var key = new Date(year, month, day);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(7, files.Length);

                    var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    foreach (var file in files)
                    {
                        using (var fs = File.OpenRead(files[0]))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 24);
                            }
                        }
                    }
                }
            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task SimpleTransformation_PartitionByHour()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                        {
                            var orderedAt = baseline.AddMinutes(i);
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var day = orderDate.getDay();
var hour = orderDate.getHours();
var key = new Date(year, month, day, hour);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(24, files.Length);

                    var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    foreach (var file in files)
                    {
                        using (var fs = File.OpenRead(files[0]))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 60);
                            }
                        }
                    }
                }
            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task CanHandleMissingFieldsOnSomeDocuments()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}", 
                                OrderedAt = baseline.AddDays(i), 
                                Company = $"companies/{i}"
                            };

                            if (i % 2 == 0)
                                o.Freight = i;

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);


                    var script = @"
var o = {
    Company : this.Company
};

if (this.Freight > 0)
    o.Freight = this.Freight

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "Company", "Freight", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in  parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                continue;

                            var count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case ParquetTransformedItems.DefaultIdColumn:
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "Company":
                                        Assert.Equal($"companies/{count}", val);
                                        break;
                                    case "Freight":
                                        long expected = count % 2 == 0 ? count : 0;
                                        Assert.Equal(expected, val);
                                        break;
                                }

                                count++;

                            }
                        }
                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task CanHandleNullFieldValuesOnSomeDocument()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                            };

                            if (i % 2 == 0)
                                o.Company = "companies/" + i;

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                continue;

                            var count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case ParquetTransformedItems.DefaultIdColumn:
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "Company":
                                        string expected = count % 2 == 0 ? $"companies/{count}" : null;
                                        Assert.Equal(expected, val);
                                        break;
                                }

                                count++;

                            }
                        }
                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task ShouldRespectEtlRunFrequency()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                Company = $"companies/{i}"
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path, "*/2 * * * *"); // every 2nd minute
                    etlDone.Wait(TimeSpan.FromMinutes(1));
                    var sw = new Stopwatch();
                    sw.Start();

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                continue;

                            var count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case ParquetTransformedItems.DefaultIdColumn:
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "Company":
                                        Assert.Equal($"companies/{count}", val);
                                        break;
                                }

                                count++;

                            }
                        }
                    }

                    await Task.Delay(1000);

                    baseline = new DateTime(2021, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 20; i <= 30; i++)
                        {
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                Company = $"companies/{i}"
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                    etlDone.Wait(TimeSpan.FromSeconds(120));
                    var timeWaited = sw.Elapsed.TotalMilliseconds;
                    sw.Stop();

                    Assert.True(timeWaited > TimeSpan.FromSeconds(60).TotalMilliseconds);

                    files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task CanUseSettingFromScript()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var transformationScript = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    var connectionStringName = $"{store.Database} to S3";

                    var scriptPath = GenerateConfigurationScript(path, out string command);
                    var connectionString = new OlapConnectionString
                    {
                        Name = connectionStringName,
                        LocalSettings = new LocalSettings
                        {
                            GetBackupConfigurationScript = new GetBackupConfigurationScript
                            {
                                Exec = command, 
                                Arguments = scriptPath
                            }
                        }
                    };

                    var configuration = new OlapEtlConfiguration
                    {
                        Name = connectionStringName,
                        ConnectionStringName = connectionStringName,
                        RunFrequency = DefaultFrequency,
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = "MonthlyOrders",
                                Collections = new List<string> {"Orders"},
                                Script = transformationScript
                            }
                        },
                        KeepFilesOnDisk = true
                    };

                    AddEtl(store, configuration, connectionString);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);
                }
            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task AfterDatabaseRestartEtlShouldRespectRunFrequency()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                Company = $"companies/{i}"
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    var sw = new Stopwatch();
                    sw.Start();
                    SetupLocalOlapEtl(store, script, path, "*/3 * * * *"); // every 3rd minute
                    Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length); 

                    // disable and re enable the database

                    var result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                    Assert.True(result.Success);
                    Assert.True(result.Disabled);

                    result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                    Assert.True(result.Success);
                    Assert.False(result.Disabled);

                    etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    baseline = new DateTime(2021, 1, 1);

                    // add more data
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 20; i <= 30; i++)
                        {
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                Company = $"companies/{i}"
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    Assert.False(etlDone.Wait(TimeSpan.FromSeconds(50)));
                    files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    Assert.True(etlDone.Wait(TimeSpan.FromSeconds(180)));

                    var timeWaited = sw.Elapsed.TotalMilliseconds;
                    sw.Stop();

                    Assert.True(timeWaited > TimeSpan.FromSeconds(60).TotalMilliseconds, "time : " + sw.Elapsed);

                    files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task LastModifiedTicksShouldMatch()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    var ids = new List<string>();

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var id = $"orders/{i}";
                            ids.Add(id);
                            var o = new Query.Order
                            {
                                Id = id,
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "RequireAt", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var session = store.OpenAsyncSession())
                    {
                        var docs = await session.LoadAsync<Order>(ids.ToArray());
                        string[] idFieldData = null;
                        long?[] lsatModifiedFieldData = null;

                        using (var fs = File.OpenRead(files[0]))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 10);

                                switch (field.Name)
                                {
                                    case ParquetTransformedItems.LastModifiedColumn:
                                        lsatModifiedFieldData = (long?[])data;
                                        break;
                                    case ParquetTransformedItems.DefaultIdColumn:
                                        idFieldData = (string[])data;
                                        break;
                                    case "RequireAt":
                                        continue;
                                }
                            }
                        }

                        Assert.NotNull(idFieldData);
                        Assert.NotNull(lsatModifiedFieldData);

                        for (var index = 0; index < idFieldData.Length; index++)
                        {
                            var id = idFieldData[index];
                            Assert.True(docs.TryGetValue(id, out var doc));

                            var expectedTicks = session.Advanced.GetLastModifiedFor(doc)?.Ticks;
                            Assert.Equal(expectedTicks, lsatModifiedFieldData[index]);
                        }
                    }

                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task CanModifyIdColumnName()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    const string idColumn = "OrderId";

                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                    var connectionStringName = $"{store.Database} to local";
                    var configuration = new OlapEtlConfiguration
                    {
                        Name = "olap-test",
                        ConnectionStringName = connectionStringName,
                        RunFrequency = DefaultFrequency,
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = "MonthlyOrders",
                                Collections = new List<string> {"Orders"},
                                Script = script
                            }
                        },
                        OlapTables = new List<OlapEtlTable>()
                        {
                            new OlapEtlTable
                            {
                                TableName = "Orders",
                                DocumentIdColumn = idColumn
                            }
                        },

                        KeepFilesOnDisk = true
                    };


                    SetupLocalOlapEtl(store, configuration, path, connectionStringName);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "RequireAt", "Total", idColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                continue;

                            long count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case idColumn:
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "RequireAt":
                                        var expected = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count).AddDays(7), DateTimeKind.Utc));
                                        Assert.Equal(expected, val);
                                        break;
                                    case "Total":
                                        var expectedTotal = count * count * 10;
                                        Assert.Equal(expectedTotal, val);
                                        break;
                                }

                                count++;

                            }
                        }
                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task SimpleTransformation_NoPartition()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1).ToUniversalTime();

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                ShipVia = $"shippers/{i}",
                                Company = $"companies/{i}"
                            });
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
loadToOrders(noPartition(),
    {
        OrderDate : this.OrderedAt
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";
                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "OrderDate", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    foreach (var fileName in files)
                    {
                        using (var fs = File.OpenRead(fileName))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));
                                var data = rowGroupReader.ReadColumn((DataField)field).Data;

                                Assert.True(data.Length == 100);

                                if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                    continue;

                                var count = 0;
                                foreach (var val in data)
                                {
                                    if (field.Name == "OrderDate")
                                    {
                                        var expectedDto = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count), DateTimeKind.Utc));
                                        Assert.Equal(expectedDto, val);
                                    }

                                    else
                                    {
                                        var expected = field.Name switch
                                        {
                                            ParquetTransformedItems.DefaultIdColumn => $"orders/{count}",
                                            "Company" => $"companies/{count}",
                                            "ShipVia" => $"shippers/{count}",
                                            _ => null
                                        };

                                        Assert.Equal(expected, val);
                                    }

                                    count++;
                                }

                            }

                        }

                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        [Fact]
        public async Task SimpleTransformation_MultiplePartitions()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = DateTime.SpecifyKind(new DateTime(2020, 1, 1), DateTimeKind.Utc);

                    using (var session = store.OpenAsyncSession())
                    {
                        const int total = 31 + 28; // days in January + days in February 

                        for (int i = 0; i < total; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                ShipVia = $"shippers/{i}",
                                Company = $"companies/{i}"
                            });
                        }

                        for (int i = 1; i <= 37; i++)
                        {
                            var index = i + total;
                            var orderedAt = baseline.AddYears(1).AddMonths(1).AddDays(i);
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{index}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                ShipVia = $"shippers/{index}",
                                Company = $"companies/{index}"
                            });
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy([
    ['year', orderDate.getFullYear()],
    ['month', orderDate.getMonth()]
]),
    {
        Company : this.Company,
        ShipVia : this.ShipVia,
        RequireAt : this.RequireAt
    });
";
                    SetupLocalOlapEtl(store, script, path);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(4, files.Length);

                    var expectedFields = new[] { "RequireAt", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    foreach (var fileName in files)
                    {
                        using (var fs = File.OpenRead(fileName))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));
                                var data = rowGroupReader.ReadColumn((DataField)field).Data;

                                Assert.True(data.Length == 31 || data.Length == 28 || data.Length == 27 || data.Length == 10);
                                if (field.Name != "RequireAt")
                                    continue;

                                var count = data.Length switch
                                {
                                    31 => 0, 
                                    28 => 31, 
                                    27 => 365 + 33,
                                    10 => 365 + 33 + 27
                                };

                                foreach (var val in data)
                                {
                                    var expectedOrderDate = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count++), DateTimeKind.Utc));
                                    var expected = expectedOrderDate.AddDays(7);
                                    Assert.Equal(expected, val);
                                }

                            }

                        }

                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        private static string GenerateConfigurationScript(string path, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var localSetting = new LocalSettings
            {
                FolderPath = path
            };

            var localSettingsString = JsonConvert.SerializeObject(localSetting);

            string script;

            if (PlatformDetails.RunningOnPosix)
            {
                command = "bash";
                script = $"#!/bin/bash\r\necho '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                command = "powershell";
                script = $"echo '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
            }

            return scriptPath;
        }

        private static string GetTempPath(string collection, [CallerMemberName] string caller = null)
        {
            var tmpPath = Path.GetTempPath();
            return Directory.CreateDirectory(Path.Combine(tmpPath, caller, collection)).FullName;
        }

        private void SetupLocalOlapEtl(DocumentStore store, string script, string path, string frequency = null)
        {
            var connectionStringName = $"{store.Database} to local";
            var configuration = new OlapEtlConfiguration
            {
                Name = "olap-test",
                ConnectionStringName = connectionStringName,
                RunFrequency = frequency ?? DefaultFrequency
                ,
                Transforms =
                {
                    new Transformation
                    {
                        Name = "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                },
                KeepFilesOnDisk = true
            };

            SetupLocalOlapEtl(store, configuration, path, connectionStringName);
        }

        private void SetupLocalOlapEtl(DocumentStore store, OlapEtlConfiguration configuration, string path, string connectionStringName)
        {
            var connectionString = new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            AddEtl(store, configuration, connectionString);
        }

    }
}

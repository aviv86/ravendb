#if NET8_0
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Xunit;

namespace EmbeddedTests.Server.Integrations.PostgreSQL;

public class RavenDB_17503 : PostgreSqlIntegrationTestBase
{
    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_single_column_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            const string sql = @"select ""_"".""Name""
from 
(
    select ""rows"".""Name"" as ""Name""
    from 
    (
        select ""Name""
        from 
        (
            declare function name(e) {
    return e.FirstName + "" "" + e.LastName
}
from Employees as e
where id() in ('employees/1-A'  )
select { Name: name(e)}
        ) ""$Table""
    ) ""rows""
    group by ""Name""
) ""_""
order by ""_"".""Name""
limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(1, result.Rows.Count);
            Assert.Equal("Nancy Davolio", result.Rows[0]["Name"]);
        }
    }

    [Fact]
    public async Task DirectQuery_distinct_list_wrapper_two_columns_should_work_end_to_end()
    {
        using (var store = GetDocumentStore())
        {

            await store.Maintenance.SendAsync(new CreateSampleDataOperation());

            const string sql = @"select ""_"".""Name"",
        ""_"".""Title""
    from
    (
        select ""rows"".""Name"" as ""Name"",
            ""rows"".""Title"" as ""Title""
        from
        (
            select ""Name"",
                ""Title""
            from
            (
                declare function name(e) {
        return e.FirstName + "" "" + e.LastName
    }
    from Employees as e
    where id() in ('employees/1-A','employees/2-A','employees/3-A')
    select { Name: name(e), Title: e.Title}
            ) ""$Table""
        ) ""rows""
        group by ""Name"",
            ""Title""
    ) ""_""
    order by ""_"".""Name"",
            ""_"".""Title""
    limit 501";

            var result = await Act(store, sql);

            Assert.NotNull(result);
            Assert.NotEmpty(result.Rows);
            Assert.Equal(3, result.Rows.Count);

            Assert.True(result.Columns.Contains("Name"));
            Assert.True(result.Columns.Contains("Title"));

            var actual = new HashSet<(string Name, string Title)>();
            foreach (DataRow row in result.Rows)
            {
                Assert.True(row.Table.Columns.Contains("Name"));
                Assert.True(row.Table.Columns.Contains("Title"));

                Assert.NotNull(row["Name"]);
                Assert.NotNull(row["Title"]);

                Assert.NotEqual(System.DBNull.Value, row["Name"]);
                Assert.NotEqual(System.DBNull.Value, row["Title"]);

                actual.Add(((string)row["Name"], (string)row["Title"]));
            }

            Assert.Contains(("Nancy Davolio", "Sales Representative"), actual);
            Assert.Contains(("Andrew Fuller", "Vice President, Sales"), actual);
            Assert.Contains(("Janet Leverling", "Sales Representative"), actual);
        }
    }
}
#endif

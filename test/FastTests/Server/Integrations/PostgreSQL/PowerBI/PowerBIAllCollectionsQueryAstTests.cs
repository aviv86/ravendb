using System;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.PowerBI;

public sealed class PowerBIAllCollectionsQueryAstTests
{
    [Fact]
    public void TryParse_should_match_powerbi_all_collections_query_shape()
    {
        const string sql = "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n" +
                           "from INFORMATION_SCHEMA.tables\n" +
                           "where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\n" +
                           "order by TABLE_SCHEMA, TABLE_NAME";

        var ok = PowerBIAllCollectionsQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
        Assert.True(ok);
        Assert.IsType<PowerBIAllCollectionsQuery>(pgQuery);
    }

    [Fact]
    public void TryParse_should_match_with_different_casing_and_whitespace()
    {
        const string sql = "  SeLeCt  t.\"TABLE_SCHEMA\"  ,\n\tt.TaBlE_NaMe,   t.TABLE_TYPE\n\n" +
                           "FrOm\n  information_schema . tables   t\n" +
                           "wHeRe  NOT ( t.TABLE_SCHEMA   in  ( 'INFORMATION_SCHEMA'  ,  'PG_CATALOG' ) )\n" +
                           "OrDeR\n  by  table_schema ,\n table_name  ";

        var ok = PowerBIAllCollectionsQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
        Assert.True(ok);
        Assert.IsType<PowerBIAllCollectionsQuery>(pgQuery);
    }

    [Fact]
    public void TryParse_should_reject_when_query_shape_does_not_match()
    {
        const string sql = "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n" +
                           "from INFORMATION_SCHEMA.tables\n" +
                           "where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\n" +
                           "order by TABLE_NAME, TABLE_SCHEMA";

        var ok = PowerBIAllCollectionsQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
        Assert.False(ok);
        Assert.Null(pgQuery);
    }

    [Fact]
    public void TryParse_should_match_with_qualified_select_and_order_by()
    {
        const string sql = "select t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE\n" +
                           "from information_schema.tables t\n" +
                           "where t.TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\n" +
                           "order by t.TABLE_SCHEMA, t.TABLE_NAME";

        var ok = PowerBIAllCollectionsQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
        Assert.True(ok);
        Assert.IsType<PowerBIAllCollectionsQuery>(pgQuery);
    }

    [Fact]
    public void TryParse_should_match_powerbi_all_collections_query_shape_with_not_wrapper_variant()
    {
        const string sql = "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n" +
                           "from INFORMATION_SCHEMA.tables\n" +
                           "where NOT (TABLE_SCHEMA in ('information_schema', 'pg_catalog'))\n" +
                           "order by TABLE_SCHEMA, TABLE_NAME";

        var ok = PowerBIAllCollectionsQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
        Assert.True(ok);
        Assert.IsType<PowerBIAllCollectionsQuery>(pgQuery);
    }
}

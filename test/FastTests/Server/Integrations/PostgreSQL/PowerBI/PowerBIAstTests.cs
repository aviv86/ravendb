using System;
using System.Reflection;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBIAstTests
    {
        [Fact]
        public void Simple_public_table_fetch_should_translate_via_ast_and_strip_table_alias_and_json_projection()
        {
            const string sql = @"select ""$Table"".""id()"" as ""id()"", ""$Table"".""Company"" as ""Company"", ""$Table"".""json()"" as ""json()""
from ""public"".""Orders"" ""$Table"" limit 200";

            Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.Contains("select", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("id()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("$Table.", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("json()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 200", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void TryParse_should_match_powerbi_all_collections_query_shape()
        {
            const string sql = "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\n" +
                               "from INFORMATION_SCHEMA.tables\n" +
                               "where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\n" +
                               "order by TABLE_SCHEMA, TABLE_NAME";

            Assert.True(PowerBIAllCollectionsQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIAllCollectionsQuery>(pgQuery);
        }

        [Fact]
        public void TryParse_should_match_preview_columns_query_shape_and_extract_table_name()
        {
            const string sql = "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, " +
                               "case when DATA_TYPE like '%char%' then DATA_TYPE else DATA_TYPE end as DATA_TYPE\n" +
                               "from INFORMATION_SCHEMA.columns\n" +
                               "where TABLE_SCHEMA = 'public' and TABLE_NAME = 'Regions'\n" +
                               "order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

            Assert.True(PowerBIPreviewQuery.TryParse(sql, documentDatabase: null, out var pgQuery));
            Assert.IsType<PowerBIPreviewQuery>(pgQuery);
            Assert.Equal("from 'Regions'", GetQueryString(pgQuery));
        }

        private static string GetQueryString(PgQuery pgQuery)
        {
            return (string)typeof(PgQuery)
                .GetField("QueryString", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(pgQuery);
        }

    }
}

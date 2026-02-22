using System;
using System.Reflection;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBIAstTests
    {
        private static string GetQueryString(PgQuery pgQuery)
        {
            return (string)typeof(PgQuery)
                .GetField("QueryString", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(pgQuery);
        }

        [Fact]
        public void Simple_public_table_fetch_should_translate_via_ast_and_strip_table_alias_and_json_projection()
        {
            const string sql = @"select ""$Table"".""id()"" as ""id()"", ""$Table"".""Company"" as ""Company"", ""$Table"".""json()"" as ""json()""
from ""public"".""Orders"" ""$Table"" limit 200";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

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
        public void Translator_should_ignore_json_projection_and_emit_id_function_in_powerbi_shape()
        {
            const string sql = @"select ""$Table"".""id()"" as ""id()"", ""$Table"".""Company"" as ""Company"", ""$Table"".""json()"" as ""json()""
from ""public"".""Orders"" ""$Table"" limit 200";

            Assert.True(Raven.Server.Integrations.PostgreSQL.Translation.AstSqlToRqlTranslator.TryParse(sql, Array.Empty<int>(), out var rql));
            Assert.DoesNotContain("$Table.", rql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("json()", rql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("id()", rql, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 200", rql, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Simple_public_table_fetch_with_where_should_translate_via_ast()
        {
            const string sql = @"select ""$Table"".""Company"" as ""Company"", ""$Table"".""json()"" as ""json()""
from ""public"".""Orders"" ""$Table""
where ""$Table"".""Company"" = 'Around the Horn'
limit 200";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.Contains("where", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("$Table.", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("json()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 200", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Simple_public_table_fetch_with_order_by_should_translate_via_ast()
        {
            const string sql = @"select ""$Table"".""Company"" as ""Company"", ""$Table"".""json()"" as ""json()""
from ""public"".""Orders"" ""$Table""
order by ""$Table"".""Company"" desc
limit 200";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.Contains("order by", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("$Table.", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("json()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 200", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Simple_public_table_fetch_selecting_only_json_should_not_emit_invalid_select_clause()
        {
            const string sql = @"select ""$Table"".""json()"" as ""json()""
from ""public"".""Orders"" ""$Table"" limit 10";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.DoesNotContain("json()", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("select", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 10", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Simple_public_table_fetch_with_offset_and_limit_should_translate_via_ast()
        {
            const string sql = @"select ""$Table"".""Company"" as ""Company""
from ""public"".""Orders"" ""$Table"" offset 10 limit 20";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.Contains("limit 10, 20", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Simple_public_table_fetch_where_is_null_should_translate_via_ast()
        {
            const string sql = @"select ""$Table"".""Company"" as ""Company""
from ""public"".""Orders"" ""$Table""
where ""$Table"".""Company"" is null
limit 5";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.DoesNotContain("$Table.", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("= null", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 5", queryString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void Simple_public_table_fetch_where_in_should_translate_via_ast()
        {
            const string sql = @"select ""$Table"".""Company"" as ""Company""
from ""public"".""Orders"" ""$Table""
where ""$Table"".""Company"" in ('A', 'B')
limit 5";

            var ok = PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), documentDatabase: null, out var pgQuery);
            Assert.True(ok);

            var rqlQuery = Assert.IsType<PowerBIRqlQuery>(pgQuery);
            var queryString = GetQueryString(rqlQuery);

            Assert.NotNull(queryString);
            Assert.Contains("from 'Orders'", queryString);
            Assert.DoesNotContain("$Table.", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("in", queryString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("limit 0, 5", queryString, StringComparison.OrdinalIgnoreCase);
        }
    }
}

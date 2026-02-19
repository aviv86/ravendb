using System;
using Raven.Server.Integrations.PostgreSQL.Translation;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.Translation
{
    public sealed class AstSqlToRqlTranslatorTests
    {
        private static string Translate(string sql)
        {
            Assert.True(AstSqlToRqlTranslator.TryTranslate(sql, Array.Empty<int>(), out var rql));
            return rql;
        }

        // Easy (10)

        [Fact]
        public void Easy_01_SelectAllFromUsers()
        {
            var sql = "SELECT * FROM users";
            var expected = "from 'users'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_02_WhereAmountGreaterThan()
        {
            var sql = "SELECT * FROM orders WHERE amount > 10";
            var expected = "from 'orders' where amount > 10";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_03_WhereNameEqualsString()
        {
            var sql = "SELECT * FROM users WHERE name = 'ayende'";
            var expected = "from 'users' where name = 'ayende'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_04_WhereActiveTrue()
        {
            var sql = "SELECT * FROM users WHERE active = true";
            var expected = "from 'users' where active = true";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_05_WhereStatusNotEqualsString()
        {
            var sql = "SELECT * FROM orders WHERE status <> 'Cancelled'";
            var expected = "from 'orders' where status != 'Cancelled'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_06_WhereAnd()
        {
            var sql = "SELECT * FROM orders WHERE status = 'Pending' AND amount > 10";
            var expected = "from 'orders' where status = 'Pending' and amount > 10";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_07_WhereOr()
        {
            var sql = "SELECT * FROM orders WHERE status = 'Pending' OR status = 'Shipped'";
            var expected = "from 'orders' where status = 'Pending' or status = 'Shipped'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_08_OrderByDesc()
        {
            var sql = "SELECT * FROM users ORDER BY name DESC";
            var expected = "from 'users' order by name desc";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_09_LimitOffset()
        {
            var sql = "SELECT * FROM users LIMIT 10 OFFSET 20";
            var expected = "from 'users' limit 20, 10";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Easy_10_OrderByDescLimit()
        {
            var sql = "SELECT * FROM orders ORDER BY createdAt LIMIT 5";
            var expected = "from 'orders' order by createdat limit 0, 5";

            Assert.Equal(expected, Translate(sql));
        }

        // Mid (10)

        [Fact]
        public void Mid_11_WhereDottedPathEquals()
        {
            var sql = "SELECT * FROM orders WHERE ShipTo.City = 'London'";
            var expected = "from 'orders' where shipto.city = 'London'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_12_Between()
        {
            var sql = "SELECT * FROM orders WHERE amount BETWEEN 10 AND 20";
            var expected = "from 'orders' where amount between 10 and 20";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_13_InList()
        {
            var sql = "SELECT * FROM orders WHERE status IN ('Pending','Shipped')";
            var expected = "from 'orders' where status in ('Pending', 'Shipped')";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_14_InListOnDottedPath()
        {
            var sql = "SELECT * FROM orders WHERE shipTo.city IN ('London','Paris')";
            var expected = "from 'orders' where shipto.city in ('London', 'Paris')";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_15_ParenthesesAndOr()
        {
            var sql = "SELECT * FROM orders WHERE (status = 'Pending' OR status = 'Shipped') AND amount > 10";
            var expected = "from 'orders' where (status = 'Pending' or status = 'Shipped') and amount > 10";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_16_OrderByTwoFields()
        {
            var sql = "SELECT * FROM orders ORDER BY createdAt DESC, amount ASC";
            var expected = "from 'orders' order by createdat desc, amount";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_17_OrderByLimit()
        {
            var sql = "SELECT * FROM users WHERE name <> 'oren' ORDER BY name LIMIT 20";
            var expected = "from 'users' where name != 'oren' order by name limit 0, 20";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_18_AndWithDottedPath()
        {
            var sql = "SELECT * FROM orders WHERE status = 'Pending' AND shipTo.city = 'London'";
            var expected = "from 'orders' where status = 'Pending' and shipto.city = 'London'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_19_IsNull()
        {
            var sql = "SELECT * FROM orders WHERE shippedAt IS NULL";
            var expected = "from 'orders' where shippedat = null";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Mid_20_AndWithParentheses()
        {
            var sql = "SELECT * FROM users WHERE active = true AND (name = 'ayende' OR name = 'oren')";
            var expected = "from 'users' where active = true and (name = 'ayende' or name = 'oren')";

            Assert.Equal(expected, Translate(sql));
        }

        // Complex (10)

        [Fact]
        public void Complex_21_SelectColumns()
        {
            var sql = "SELECT id, name FROM users";
            var expected = "from 'users' select id, name";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_22_SelectColumnsWithWhere()
        {
            var sql = "SELECT id, status, shipTo.city FROM orders WHERE amount > 10";
            var expected = "from 'orders' where amount > 10 select id, status, shipto.city";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_23_CountStar()
        {
            var sql = "SELECT COUNT(*) FROM orders";
            var expected = "from 'orders' select count()";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_24_Sum()
        {
            var sql = "SELECT COUNT(*), SUM(amount), AVG(score) FROM orders";
            var expected = "from 'orders' select count(), sum(amount), avg(score)";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_25_AvgWithWhere()
        {
            var sql = "SELECT AVG(amount) FROM orders WHERE status = 'Paid'";
            var expected = "from 'orders' where status = 'Paid' select avg(amount)";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_26_GroupByCount()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";
            var expected = "from 'orders' group by status select status, count()";
            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_27_GroupByCountOrderByCountDesc()
        {
            var sql = "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY COUNT(*) DESC";
            var expected = "from 'orders' group by status order by 'count()' desc select status, count()";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_28_Distinct()
        {
            var sql = "SELECT DISTINCT status FROM orders";
            var expected = "from 'orders' select distinct status";
            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_29_IndexQueryContains()
        {
            var sql = "SELECT * FROM indexes.\"Users/ByName\" WHERE name = 'oren'";
            var expected = "from index 'Users/ByName' where name = 'oren'";

            Assert.Equal(expected, Translate(sql));
        }

        [Fact]
        public void Complex_30_Join()
        {
            var sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
            var expected = "from 'orders' as o load o.user_id as u select { o: o, u: u }";

            Assert.Equal(expected, Translate(sql));
        }
    }
}

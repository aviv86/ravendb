using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PgSqlParser;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;
using Node = PgSqlParser.Node;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBIAllCollectionsQuery : PowerBIRqlQuery
    {
        public PowerBIAllCollectionsQuery(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase)
            : base(queryText, parametersDataTypes, documentDatabase)
        {
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            pgQuery = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            try
            {
                var parseResult = Parser.Parse(queryText);
                if (parseResult.IsSuccess == false || parseResult.Value?.Stmts == null)
                    return false;

                if (parseResult.Value.Stmts.Count != 1)
                    return false;

                var stmt = parseResult.Value.Stmts[0];
                var selectStmt = stmt?.Stmt?.SelectStmt;
                if (selectStmt == null)
                    return false;

                if (IsPowerBiAllCollectionsShape(selectStmt) == false)
                    return false;

                pgQuery = new PowerBIAllCollectionsQuery(queryText, parametersDataTypes, documentDatabase);
                return true;
            }
            catch
            {
                pgQuery = null;
                return false;
            }
        }

        private static bool IsPowerBiAllCollectionsShape(SelectStmt selectStmt)
        {
            if (selectStmt?.FromClause is not { Count: 1 })
                return false;

            var fromNode = selectStmt.FromClause[0];
            if (fromNode?.RangeVar == null)
                return false;

            var rangeVar = fromNode.RangeVar;
            if (rangeVar.Schemaname == null || rangeVar.Relname == null)
                return false;

            if (rangeVar.Schemaname.Equals("information_schema", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (rangeVar.Relname.Equals("tables", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (selectStmt.TargetList is not { Count: 3 })
                return false;

            if (IsSelectColumn(selectStmt.TargetList[0], "TABLE_SCHEMA") == false)
                return false;

            if (IsSelectColumn(selectStmt.TargetList[1], "TABLE_NAME") == false)
                return false;

            if (IsSelectColumn(selectStmt.TargetList[2], "TABLE_TYPE") == false)
                return false;

            if (TryMatchTableSchemaNotInWhereClause(selectStmt.WhereClause) == false)
                return false;

            if (selectStmt.SortClause is not { Count: 2 })
                return false;

            if (IsOrderByColumn(selectStmt.SortClause[0], "TABLE_SCHEMA") == false)
                return false;

            if (IsOrderByColumn(selectStmt.SortClause[1], "TABLE_NAME") == false)
                return false;

            return true;

            static bool IsSelectColumn(Node node, string expectedColumn)
            {
                var resTarget = node?.ResTarget;
                if (resTarget == null)
                    return false;

                var colRef = resTarget.Val?.ColumnRef;
                if (colRef == null || colRef.Fields.Count != 1)
                    return false;

                return colRef.Fields[0].String.Sval.Equals(expectedColumn, StringComparison.OrdinalIgnoreCase);
            }

            static bool IsOrderByColumn(Node node, string expectedColumn)
            {
                var sortBy = node?.SortBy;
                if (sortBy == null)
                    return false;

                if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                    return false;

                var colRef = sortBy.Node?.ColumnRef;
                if (colRef == null)
                    return false;

                return colRef.Fields[0].String.Sval.Equals(expectedColumn, StringComparison.OrdinalIgnoreCase);
            }


            static bool TryMatchTableSchemaNotInWhereClause(Node whereClause)
            {
                var aExpr = whereClause?.AExpr;
                if (aExpr == null)
                    return false;

                if (aExpr.Kind != A_Expr_Kind.AexprIn)
                    return false;

                // check left side is "TABLE_SCHEMA"
                if (aExpr.Lexpr?.ColumnRef.Fields.Count != 1 || 
                    aExpr.Lexpr?.ColumnRef.Fields[0].String?.Sval?.Equals("TABLE_SCHEMA", StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                if (aExpr.Rexpr?.List?.Items is not { Count: 2 } items)
                    return false;

                // check values in the list are 'information_schema' and 'pg_catalog'
                string v0 = items[0]?.AConst?.Sval?.Sval;
                string v1 = items[1]?.AConst?.Sval?.Sval;

                if (v0?.Equals("information_schema", StringComparison.OrdinalIgnoreCase) == false ||
                    v1?.Equals("pg_catalog", StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                // check operator is "<>"
                if (aExpr.Name is not { Count: 1 } || aExpr?.Name[0].String?.Sval == null)
                    return false;

                return aExpr.Name[0].String.Sval.Equals("<>");
            }
        }

        public override Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            return Task.FromResult<ICollection<PgColumn>>(new PgColumn[]
            {
                new("table_schema", 0, PgName.Default, PgFormat.Text),
                new("table_name", 1, PgName.Default, PgFormat.Text),
                new("table_type", 2, PgVarchar.Default, PgFormat.Text),
            });
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            var collections = new List<string>();

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in DocumentDatabase.DocumentsStorage.GetCollections(context))
                {
                    if (CollectionName.IsHiLoCollection(collection.Name))
                        continue;

                    collections.Add(collection.Name);
                }
            }

            foreach (var collection in collections)
            {
                var dataRow = new ReadOnlyMemory<byte>?[]
                {
                    Encoding.UTF8.GetBytes("public"),
                    Encoding.UTF8.GetBytes(collection),
                    Encoding.UTF8.GetBytes("BASE TABLE"),
                };

                await writer.WriteAsync(builder.DataRow(dataRow), token);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {collections.Count}"), token);
        }
    }
}

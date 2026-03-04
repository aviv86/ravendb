using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PgSqlParser;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBIPreviewQuery : PowerBIRqlQuery
    {
        private readonly List<PgDataRow> _results;

        public PowerBIPreviewQuery(DocumentDatabase documentDatabase, string tableName)
            : base($"from '{tableName}'", Array.Empty<int>(), documentDatabase, limit: 1)
        {
            _results = new List<PgDataRow>();
        }

        public static bool TryParse(string queryText, DocumentDatabase documentDatabase, out PgQuery pgQuery)
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

                if (IsPowerBiPreviewShape(selectStmt, out var tableName) == false)
                    return false;

                pgQuery = new PowerBIPreviewQuery(documentDatabase, tableName);
                return true;
            }
            catch
            {
                pgQuery = null;
                return false;
            }
        }

        private static bool IsPowerBiPreviewShape(SelectStmt selectStmt, out string tableName)
        {
            tableName = null;

            if (selectStmt?.FromClause is not { Count: 1 })
                return false;

            var fromNode = selectStmt.FromClause[0];

            var rangeVar = fromNode?.RangeVar;
            if (rangeVar?.Schemaname == null || rangeVar.Relname == null)
                return false;

            if (rangeVar.Schemaname.Equals("information_schema", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (rangeVar.Relname.Equals("columns", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (selectStmt.TargetList is not { Count: >= 3 } targetList)
                return false;

            if (IsSelectColumn(targetList[0], "COLUMN_NAME") == false)
                return false;

            if (IsSelectColumn(targetList[1], "ORDINAL_POSITION") == false)
                return false;

            if (IsSelectColumn(targetList[2], "IS_NULLABLE") == false)
                return false;

            static bool IsSelectColumn(Node node, string expectedColumn)
            {
                var colRef = node?.ResTarget?.Val?.ColumnRef;
                return colRef?.Fields is { Count: 1 } &&
                       colRef.Fields[0].String?.Sval?.Equals(expectedColumn, StringComparison.OrdinalIgnoreCase) == true;
            }

            if (TryExtractWhereTableSchemaAndName(selectStmt.WhereClause, out var schemaName, out tableName) == false)
                return false;

            if (schemaName.Equals("public", StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (ContainsOrderByOrdinalPosition(selectStmt.SortClause) == false)
                return false;

            return true;

            static bool TryExtractWhereTableSchemaAndName(Node whereNode, out string schemaName, out string tableName)
            {
                schemaName = null;
                tableName = null;

                if (whereNode?.BoolExpr is not { Boolop: BoolExprType.AndExpr, Args: { Count: 2 } args })
                    return false;

                var tableSchemaExpr = args[0]?.AExpr;
                if (tableSchemaExpr == null)
                    return false;

                if (tableSchemaExpr.Kind != A_Expr_Kind.AexprOp)
                    return false;

                if (tableSchemaExpr.Name is not { Count: 1 } || tableSchemaExpr.Name[0].String?.Sval != "=")
                    return false;

                var tableSchemaColRef = tableSchemaExpr.Lexpr?.ColumnRef;
                if (tableSchemaColRef?.Fields is not { Count: 1 })
                    return false;

                if (tableSchemaColRef.Fields[0].String?.Sval?.Equals("TABLE_SCHEMA", StringComparison.OrdinalIgnoreCase) != true)
                    return false;

                schemaName = tableSchemaExpr.Rexpr?.AConst?.Sval?.Sval;
                if (string.IsNullOrWhiteSpace(schemaName))
                    return false;

                var tableNameExpr = args[1]?.AExpr;
                if (tableNameExpr == null)
                    return false;

                if (tableNameExpr.Kind != A_Expr_Kind.AexprOp)
                    return false;

                if (tableNameExpr.Name is not { Count: 1 } || tableNameExpr.Name[0].String?.Sval != "=")
                    return false;

                var tableNameColRef = tableNameExpr.Lexpr?.ColumnRef;
                if (tableNameColRef?.Fields is not { Count: 1 })
                    return false;

                if (tableNameColRef.Fields[0].String?.Sval?.Equals("TABLE_NAME", StringComparison.OrdinalIgnoreCase) != true)
                    return false;

                tableName = tableNameExpr.Rexpr?.AConst?.Sval?.Sval;
                return string.IsNullOrWhiteSpace(tableName) == false;
            }

            static bool ContainsOrderByOrdinalPosition(IList<Node> sortClause)
            {
                if (sortClause == null || sortClause.Count == 0)
                    return false;

                if (sortClause.Count != 3)
                    return false;

                if (IsOrderBy(sortClause[0], "TABLE_SCHEMA") == false)
                    return false;

                if (IsOrderBy(sortClause[1], "TABLE_NAME") == false)
                    return false;

                if (IsOrderBy(sortClause[2], "ORDINAL_POSITION") == false)
                    return false;

                return  true;

                static bool IsOrderBy(Node node, string expectedColumn)
                {
                    var sortBy = node?.SortBy;
                    if (sortBy == null)
                        return false;

                    if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                        return false;

                    var colRef = sortBy.Node?.ColumnRef;
                    if (colRef?.Fields is not { Count: 1 })
                        return false;

                    return colRef.Fields[0].String?.Sval?.Equals(expectedColumn, StringComparison.OrdinalIgnoreCase) == true;
                }
            }
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            foreach (var dataRow in _results)
            {
                await writer.WriteAsync(builder.DataRow(dataRow.ColumnData.Span), token);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {_results.Count}"), token);
        }

        public override void Bind(ICollection<byte[]> parameters, short[] parameterFormatCodes, short[] resultColumnFormatCodes)
        {
            // Note: We don't call base.Bind(..) because we only support parameters for this custom RQL, not the original SQL

            // Intentional no-op: this query executes our own RQL (not the original SQL) and currently has no parameters.
        }

        public override async Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            var schema = await base.Init(allowMultipleStatements);

            int i = 0;
            foreach (var column in schema)
            {
                var columnData = new ReadOnlyMemory<byte>?[]
                {
                    Encoding.UTF8.GetBytes(column.Name), // column_name
                    BitConverter.GetBytes(IPAddress.HostToNetworkOrder(i)), // ordinal_position
                    Encoding.UTF8.GetBytes("YES"), // is_nullable
                    Encoding.UTF8.GetBytes(""), // data_type - easier to leave empty for us
                };

                _results.Add(new PgDataRow(columnData));
                i++;
            }

            return new List<PgColumn>
            {
                new("column_name", 0, PgName.Default,PgFormat.Text),
                new("ordinal_position", 1, PgInt4.Default, PgFormat.Binary),
                new("is_nullable", 2, PgVarchar.Default, PgFormat.Text),
                new("data_type", 3, PgVarchar.Default, PgFormat.Text),
            };
        }
    }
}

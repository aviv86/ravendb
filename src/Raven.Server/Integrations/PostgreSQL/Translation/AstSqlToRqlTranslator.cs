using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using PgSqlParser;
using Raven.Client;
using Raven.Client.Documents.Session;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL.Translation
{
    internal static class AstSqlToRqlTranslator
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(AstSqlToRqlTranslator));

        private const string UnsupportedSelectAggregateMessage = "Unsupported SELECT aggregate";
        private const string UnsupportedSelectProjectionMessage = "Unsupported SELECT projection";
        private const string MixedAggregateAndNonAggregateMessage = "Mixing aggregates and non-aggregated columns is not supported yet.";
        private const string UnsupportedDistinctMessage = "Unsupported DISTINCT";
        private const string UnsupportedGroupByMessage = "Unsupported GROUP BY";
        private const string UnsupportedOrderByForGroupByMessage = "Unsupported ORDER BY for GROUP BY";
        private const string UnsupportedJoinMessage = "Unsupported JOIN shape";

        public static bool TryParse(string sql, int[] parameterTypes, out string rql)
        {
            // TODO RavenDB-26030: parameterTypes are currently unused by the translator.
            rql = null;

            if (Logger.IsDebugEnabled)
                Logger.Debug($"{nameof(AstSqlToRqlTranslator)}.{nameof(TryParse)} invoked with SQL: {sql}");

            try
            {
                var parseResult = Parser.Parse(sql);

                if (parseResult.IsSuccess == false || parseResult.Value == null)
                {
                    if (Logger.IsDebugEnabled)
                    {
                        var parseError = parseResult.Error;
                        if (parseError != null)
                            Logger.Debug($"{nameof(AstSqlToRqlTranslator)} parse error. Message: {parseError.Message}. SQL: {sql}");
                        else
                            Logger.Debug($"{nameof(AstSqlToRqlTranslator)} parse error. SQL: {sql}");
                    }

                    return LogFailure("parse error");
                }

                if (parseResult.Value.Stmts == null || parseResult.Value.Stmts.Count == 0)
                    throw new NotSupportedException("No statements found in query.");

                var stmt = parseResult.Value.Stmts[0];
                if (stmt?.Stmt?.SelectStmt == null)
                    throw new NotSupportedException("Only SELECT statements are supported.");

                rql = TranslateSelectStatement(stmt.Stmt.SelectStmt);
                return LogSuccess(sql, rql);
            }
            catch (NotSupportedException)
            {
                rql = null;
                return LogFailure("unsupported query shape");
            }
        }

        private static bool LogSuccess(string sql, string rql)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"{nameof(AstSqlToRqlTranslator)} translated SQL to RQL. SQL: {sql}. RQL: {rql}");
            return true;
        }

        private static bool LogFailure(string reason)
        {
            if (Logger.IsDebugEnabled)
                Logger.Debug($"{nameof(AstSqlToRqlTranslator)} did not translate SQL. Reason: {reason}");
            return false;
        }

        private static string TranslateSelectStatement(SelectStmt selectStmt)
        {
            if (selectStmt.FromClause is [{ JoinExpr: not null }])
                return TranslateSimpleJoin(selectStmt);

            if (selectStmt.FromClause is not [{ RangeVar: { Relname: var relname } rangeVar }])
                throw new NotSupportedException("FROM clause with collection or index name is required");

            var isIndex = string.Equals(rangeVar.Schemaname, "indexes", StringComparison.OrdinalIgnoreCase);
            var isGroupBy = selectStmt.GroupClause != null && selectStmt.GroupClause.Count > 0;
            var q = new AsyncDocumentQuery<JObject>(session: null, indexName: isIndex ? relname : null, collectionName: isIndex ? null : relname, isGroupBy: isGroupBy);
            var fromAlias = rangeVar.Alias?.Aliasname;

            // Build WHERE clause (must be applied before GROUP BY)
            if (selectStmt.WhereClause != null)
                TranslateWhereClause(q, selectStmt.WhereClause, fromAlias);

            if (isGroupBy)
            {
                ApplyGroupBy(q, selectStmt);
            }
            else
            {
                ApplySelectProjection(q, selectStmt, fromAlias);

                // Build ORDER BY clause
                if (selectStmt.SortClause != null && selectStmt.SortClause.Count > 0)
                    TranslateOrderBy(q, selectStmt.SortClause, fromAlias);
            }

            // Build OFFSET clause
            if (selectStmt.LimitOffset != null)
            {
                var offset = TranslateLimit(selectStmt.LimitOffset);
                if (offset != null)
                    q.Skip(offset.Value);
            }

            // Build LIMIT clause
            if (selectStmt.LimitCount != null)
            {
                var limit = TranslateLimit(selectStmt.LimitCount);
                if (limit != null)
                    q.Take(limit.Value);
            }

            // Prefer the official query text emitted by IndexQuery when possible.
            // Falls back to ToString() which also returns pure RQL.
            var indexQuery = q.GetIndexQuery();

            var queryText = indexQuery?.Query ?? q.ToString();
            var parameters = indexQuery?.QueryParameters;

            if (parameters != null)
                queryText = InlineQueryParameters(queryText, parameters);

            return queryText;
        }

        private static string InlineQueryParameters(string rql, Parameters parameters)
        {
            if (string.IsNullOrWhiteSpace(rql))
                return rql;

            if (parameters == null || parameters.Count == 0)
                return rql;

            var sb = new StringBuilder(rql.Length);
            for (int i = 0; i < rql.Length; i++)
            {
                var ch = rql[i];
                if (ch != '$' || i + 2 >= rql.Length || rql[i + 1] != 'p')
                {
                    sb.Append(ch);
                    continue;
                }

                int start = i;
                i += 2;

                int numStart = i;
                while (i < rql.Length && char.IsDigit(rql[i]))
                    i++;

                if (numStart == i)
                {
                    sb.Append(rql.AsSpan(start, (i - start) + 1));
                    continue;
                }

                var paramName = rql.Substring(start + 1, i - (start + 1)); // p0
                if (parameters.TryGetValue(paramName, out var value))
                {
                    if (value is IEnumerable enumerable and not string)
                        sb.Append(FormatRqlListItems(enumerable));
                    else
                        sb.Append(FormatRqlLiteral(value));
                }
                else
                {
                    sb.Append('$');
                    sb.Append(paramName);
                }
                i--; // compensate for for-loop increment
            }

            return sb.ToString();
        }

        private static string FormatRqlLiteral(object value)
        {
            if (value == null)
                return "null";

            if (value is IEnumerable enumerable and not string)
                return FormatRqlList(enumerable);

            if (value is string s)
                return QuoteString(s);

            if (value is bool b)
                return b ? "true" : "false";

            if (value is int or long or short or byte or uint or ulong or ushort or sbyte)
                return Convert.ToString(value, CultureInfo.InvariantCulture);

            if (value is float or double or decimal)
                return Convert.ToString(value, CultureInfo.InvariantCulture);

            if (value is DateTime dt)
                return QuoteString(dt.ToString("O", CultureInfo.InvariantCulture));

            if (value is DateTimeOffset dto)
                return QuoteString(dto.ToString("O", CultureInfo.InvariantCulture));

            throw new NotSupportedException($"Unsupported query parameter type '{value.GetType()}'.");
        }

        private static string FormatRqlList(IEnumerable values)
        {
            var sb = new StringBuilder();
            sb.Append('(');

            sb.Append(FormatRqlListItems(values));

            sb.Append(')');
            return sb.ToString();
        }

        private static string FormatRqlListItems(IEnumerable values)
        {
            var sb = new StringBuilder();

            bool first = true;
            foreach (var item in values)
            {
                if (first == false)
                    sb.Append(", ");

                sb.Append(FormatRqlLiteral(item));
                first = false;
            }

            return sb.ToString();
        }

        private static string QuoteString(string s)
        {
            if (s == null)
                return "null";

            return "'" + s.Replace("'", "''", StringComparison.Ordinal) + "'";
        }

        private static string TranslateSimpleJoin(SelectStmt selectStmt)
        {
            if (selectStmt.FromClause is not [{ JoinExpr: { } joinExpr }])
                throw new NotSupportedException("FROM clause with join is required");

            if (IsSelectStar(selectStmt.TargetList ?? []) == false)
                throw new NotSupportedException(UnsupportedJoinMessage);

            if (selectStmt.WhereClause != null || selectStmt.GroupClause is { Count: > 0 } || selectStmt.SortClause is { Count: > 0 } ||
                selectStmt.LimitCount != null || selectStmt.LimitOffset != null || selectStmt.DistinctClause is { Count: > 0 })
                throw new NotSupportedException(UnsupportedJoinMessage);

            if (TryExtractSimpleJoinInfo(joinExpr, out var drivingCollection, out var drivingAlias, out var loadPath, out var loadAlias) == false)
                throw new NotSupportedException(UnsupportedJoinMessage);

            return $"from '{drivingCollection}' as {drivingAlias} load {drivingAlias}.{loadPath} as {loadAlias} select {{ {drivingAlias}: {drivingAlias}, {loadAlias}: {loadAlias} }}";
        }

        private static bool TryExtractSimpleJoinInfo(
            JoinExpr joinExpr,
            out string drivingCollection,
            out string drivingAlias,
            out string loadPath,
            out string loadAlias)
        {
            drivingCollection = string.Empty;
            drivingAlias = string.Empty;
            loadPath = string.Empty;
            loadAlias = string.Empty;

            if (TryGetRangeVarWithAlias(joinExpr.Larg, out var leftCollection, out var leftAlias) == false ||
                TryGetRangeVarWithAlias(joinExpr.Rarg, out var rightCollection, out var rightAlias) == false)
                return false;

            if (joinExpr.Quals?.AExpr is not { Kind: A_Expr_Kind.AexprOp, Name.Count: > 0 } onExpr)
                return false;

            var operatorName = GetStringValue(onExpr.Name[0]) ?? string.Empty;
            if (string.Equals(operatorName, "=", StringComparison.Ordinal) == false)
                return false;

            if (onExpr.Lexpr?.ColumnRef == null || onExpr.Rexpr?.ColumnRef == null)
                return false;

            var leftRef = ExtractFieldName(onExpr.Lexpr);
            var rightRef = ExtractFieldName(onExpr.Rexpr);
            if (string.IsNullOrWhiteSpace(leftRef) || string.IsNullOrWhiteSpace(rightRef))
                return false;

            if (TryParseQualifiedField(leftRef, out var leftRefAlias, out var leftRefField) == false ||
                TryParseQualifiedField(rightRef, out var rightRefAlias, out var rightRefField) == false)
                return false;

            if (string.Equals(leftRefField, "id", StringComparison.OrdinalIgnoreCase))
            {
                drivingAlias = rightRefAlias;
                loadPath = rightRefField;
                loadAlias = leftRefAlias;
            }
            else if (string.Equals(rightRefField, "id", StringComparison.OrdinalIgnoreCase))
            {
                drivingAlias = leftRefAlias;
                loadPath = leftRefField;
                loadAlias = rightRefAlias;
            }
            else
            {
                return false;
            }

            if (string.Equals(drivingAlias, leftAlias, StringComparison.OrdinalIgnoreCase))
                drivingCollection = leftCollection;
            else if (string.Equals(drivingAlias, rightAlias, StringComparison.OrdinalIgnoreCase))
                drivingCollection = rightCollection;
            else
                return false;

            return true;
        }

        private static bool TryGetRangeVarWithAlias(Node node, out string collection, out string alias)
        {
            collection = string.Empty;
            alias = string.Empty;

            var rv = node.RangeVar;
            if (rv?.Relname == null)
                return false;

            if (string.IsNullOrWhiteSpace(rv.Schemaname) == false)
                return false;

            collection = rv.Relname;
            alias = rv.Alias?.Aliasname ?? rv.Relname;
            return string.IsNullOrWhiteSpace(alias) == false;
        }

        private static bool TryParseQualifiedField(string field, out string alias, out string path)
        {
            alias = string.Empty;
            path = string.Empty;

            var idx = field.IndexOf('.', StringComparison.Ordinal);
            if (idx <= 0 || idx >= field.Length - 1)
                return false;

            alias = field[..idx];
            path = field[(idx + 1)..];
            return string.IsNullOrWhiteSpace(alias) == false && string.IsNullOrWhiteSpace(path) == false;
        }

        private static void ApplyGroupBy(AsyncDocumentQuery<JObject> q, SelectStmt selectStmt)
        {
            if (selectStmt.GroupClause is not { Count: 1 })
                throw new NotSupportedException(UnsupportedGroupByMessage);

            var groupKeyNode = selectStmt.GroupClause[0];
            if (groupKeyNode.ColumnRef == null)
                throw new NotSupportedException(UnsupportedGroupByMessage);

            var groupFieldName = ExtractFieldName(groupKeyNode);
            if (string.IsNullOrWhiteSpace(groupFieldName))
                throw new NotSupportedException(UnsupportedGroupByMessage);

            var targets = selectStmt.TargetList;
            if (targets == null || targets.Count == 0)
                throw new NotSupportedException(UnsupportedGroupByMessage);

            if (IsSelectStar(targets))
                throw new NotSupportedException(UnsupportedGroupByMessage);

            if (selectStmt.DistinctClause is { Count: > 0 })
                throw new NotSupportedException(UnsupportedGroupByMessage);

            var projections = new List<string>(capacity: targets.Count);
            bool hasAnyAggregate = false;

            foreach (var t in targets)
            {
                var val = t.ResTarget?.Val;
                if (val == null)
                    throw new NotSupportedException(UnsupportedGroupByMessage);

                projections.Add(BuildProjectionForGroupByTarget(val, groupFieldName));

                if (val.FuncCall != null)
                    hasAnyAggregate = true;
            }

            if (!hasAnyAggregate)
            {
                q.SelectFields<JObject>(groupFieldName);
                q.Distinct();
                return;
            }

            q.GroupBy(groupFieldName);
            q.SelectFields<JObject>(projections.ToArray());

            if (selectStmt.SortClause != null && selectStmt.SortClause.Count > 0)
                TranslateOrderByForGroupBy(q, selectStmt.SortClause, groupFieldName, projections);
        }

        private static void TranslateOrderByForGroupBy(
            AsyncDocumentQuery<JObject> q,
            Google.Protobuf.Collections.RepeatedField<Node> sortClause,
            string groupFieldName,
            IReadOnlyList<string> projections)
        {
            foreach (var sortNode in sortClause)
            {
                var sortBy = sortNode.SortBy;
                if (sortBy == null)
                    continue;

                string orderExpr = null;

                if (sortBy.Node?.ColumnRef != null)
                {
                    var fieldName = ExtractFieldName(sortBy.Node);
                    if (string.IsNullOrWhiteSpace(fieldName))
                        throw new NotSupportedException(UnsupportedOrderByForGroupByMessage);

                    if (string.Equals(fieldName, groupFieldName, StringComparison.OrdinalIgnoreCase) == false)
                        throw new NotSupportedException(UnsupportedOrderByForGroupByMessage);

                    orderExpr = projections.First(p => string.Equals(p, groupFieldName, StringComparison.OrdinalIgnoreCase));
                }
                else if (sortBy.Node?.FuncCall != null)
                {
                    var projection = BuildAggregateProjectionForGroupByOrderBy(sortBy.Node.FuncCall);

                    if (projections.Any(p => string.Equals(p, projection, StringComparison.OrdinalIgnoreCase)) == false)
                        throw new NotSupportedException(UnsupportedOrderByForGroupByMessage);

                    orderExpr = $"'{projections.First(p => string.Equals(p, projection, StringComparison.OrdinalIgnoreCase))}'";
                }
                else
                {
                    throw new NotSupportedException(UnsupportedOrderByForGroupByMessage);
                }

                if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                    q.OrderByDescending(orderExpr);
                else
                    q.OrderBy(orderExpr);
            }
        }

        private static void ApplySelectProjection(AsyncDocumentQuery<JObject> q, SelectStmt selectStmt, string fromAlias)
        {
            var targets = selectStmt.TargetList;
            if (targets == null || targets.Count == 0)
                return;

            if (IsSelectStar(targets))
                return;

            var isDistinct = selectStmt.DistinctClause is { Count: > 0 };
            var anyAgg = targets.Any(IsAggregateTarget);
            var allAgg = targets.All(IsAggregateTarget);

            if (isDistinct && anyAgg)
                throw new NotSupportedException(UnsupportedDistinctMessage);

            if (anyAgg && !allAgg)
                throw new NotSupportedException(MixedAggregateAndNonAggregateMessage);

            if (allAgg)
            {
                q.SelectFields<JObject>(BuildAggregateProjections(targets));
                return;
            }

            var projectionFields = BuildColumnProjections(targets, fromAlias);
            if (isDistinct && projectionFields.Length != 1)
                throw new NotSupportedException(UnsupportedDistinctMessage);

            if (projectionFields.Length == 0)
                return;

            q.SelectFields<JObject>(projectionFields);
            if (isDistinct)
                q.Distinct();
        }

        private static bool IsSelectStar(IReadOnlyList<Node> targetList)
        {
            return
                targetList.Count == 1 &&
                targetList[0].ResTarget?.Val?.ColumnRef?.Fields is { Count: 1 } fields &&
                fields[0].AStar != null;
        }

        private static bool IsAggregateTarget(Node target)
        {
            return target.ResTarget?.Val?.FuncCall != null;
        }

        private static string[] BuildColumnProjections(IReadOnlyList<Node> targetList, string fromAlias)
        {
            var projectionFields = new List<string>(capacity: targetList.Count);

            foreach (var t in targetList)
            {
                var val = t.ResTarget?.Val;
                if (val == null)
                    throw new NotSupportedException(UnsupportedSelectProjectionMessage);

                var fieldName = TranslateSelectTargetValue(val, fromAlias);
                if (fieldName == null)
                    continue; // e.g. PowerBI pseudo-column json()

                projectionFields.Add(fieldName);
            }

            return projectionFields.ToArray();
        }

        private static string TranslateSelectTargetValue(Node val, string fromAlias)
        {
            if (val.ColumnRef != null)
            {
                var fieldName = ExtractFieldName(val, fromAlias);
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new NotSupportedException("Unsupported column reference in SELECT projection");

                if (string.Equals(fieldName, "json()", StringComparison.OrdinalIgnoreCase))
                    return null;

                if (string.Equals(fieldName, "id()", StringComparison.OrdinalIgnoreCase))
                    return "id()";

                return fieldName;
            }

            throw new NotSupportedException(UnsupportedSelectProjectionMessage);
        }

        private static string[] BuildAggregateProjections(IReadOnlyList<Node> targetList)
        {
            var projections = new List<string>(capacity: targetList.Count);

            foreach (var t in targetList)
            {
                var funcCall = t.ResTarget?.Val?.FuncCall;
                if (funcCall == null)
                    throw new NotSupportedException(UnsupportedSelectAggregateMessage);

                var funcName = GetFuncNameOrThrow(funcCall);

                switch (funcName)
                {
                    case "count":
                        projections.Add(BuildCountProjection(funcCall));
                        break;

                    case "sum":
                    case "avg":
                        projections.Add(BuildSingleColumnAggregateProjection(funcName, funcCall));
                        break;

                    default:
                        throw new NotSupportedException(UnsupportedSelectAggregateMessage);
                }
            }

            return projections.ToArray();
        }

        private static string GetFuncNameOrThrow(FuncCall funcCall)
        {
            if (funcCall.Funcname == null || funcCall.Funcname.Count == 0 || funcCall.Funcname[0].String == null)
                throw new NotSupportedException(UnsupportedSelectAggregateMessage);

            return (funcCall.Funcname[0].String.Sval ?? string.Empty).ToLowerInvariant();
        }

        private static Node GetSingleArgOrThrow(FuncCall funcCall)
        {
            if (funcCall.Args == null || funcCall.Args.Count != 1)
                throw new NotSupportedException(UnsupportedSelectAggregateMessage);

            return funcCall.Args[0];
        }

        private static string BuildCountProjection(FuncCall funcCall)
        {
            if (funcCall.AggStar)
                return "count()";

            var countArg = GetSingleArgOrThrow(funcCall);
            if (countArg.ColumnRef != null)
            {
                var countFieldName = ExtractFieldName(countArg);
                if (string.IsNullOrWhiteSpace(countFieldName))
                    throw new NotSupportedException(UnsupportedSelectAggregateMessage);
                return $"count({countFieldName})";
            }

            if (countArg.AConst != null)
                return "count()";

            throw new NotSupportedException(UnsupportedSelectAggregateMessage);
        }

        private static string BuildSingleColumnAggregateProjection(string funcName, FuncCall funcCall)
        {
            var arg0 = GetSingleArgOrThrow(funcCall);
            if (arg0?.ColumnRef == null)
                throw new NotSupportedException(UnsupportedSelectAggregateMessage);

            var fieldName = ExtractFieldName(arg0);
            if (string.IsNullOrWhiteSpace(fieldName))
                throw new NotSupportedException(UnsupportedSelectAggregateMessage);

            return $"{funcName}({fieldName})";
        }

        private static string BuildProjectionForGroupByTarget(Node val, string groupFieldName)
        {
            if (val.ColumnRef != null)
            {
                var field = ExtractFieldName(val);
                if (string.Equals(field, groupFieldName, StringComparison.OrdinalIgnoreCase) == false)
                    throw new NotSupportedException(UnsupportedGroupByMessage);

                return groupFieldName;
            }

            if (val.FuncCall != null)
            {
                var funcName = GetFuncNameOrThrow(val.FuncCall);
                return funcName switch
                {
                    "count" => BuildCountProjection(val.FuncCall),
                    "sum" or "avg" => BuildSingleColumnAggregateProjection(funcName, val.FuncCall),
                    _ => throw new NotSupportedException(UnsupportedGroupByMessage)
                };
            }

            throw new NotSupportedException(UnsupportedGroupByMessage);
        }

        private static string BuildAggregateProjectionForGroupByOrderBy(FuncCall funcCall)
        {
            var funcName = GetFuncNameOrThrow(funcCall);
            return funcName switch
            {
                "count" => BuildCountProjection(funcCall),
                "sum" or "avg" => BuildSingleColumnAggregateProjection(funcName, funcCall),
                _ => throw new NotSupportedException(UnsupportedOrderByForGroupByMessage)
            };
        }

        private static void TranslateWhereClause(AsyncDocumentQuery<JObject> q, Node whereNode, string fromAlias, BoolExprType? parentBoolOp = null)
        {
            // BoolExpr: AND / OR (recursive)
            if (whereNode.BoolExpr != null)
            {
                var be = whereNode.BoolExpr;

                if (be.Args == null || be.Args.Count == 0)
                    throw new NotSupportedException("WHERE BoolExpr has no args");

                if (be.Boolop is not (BoolExprType.AndExpr or BoolExprType.OrExpr))
                    throw new NotSupportedException($"Unsupported BoolExpr operator: {be.Boolop}");

                var shouldWrap = parentBoolOp == BoolExprType.AndExpr && be.Boolop == BoolExprType.OrExpr;
                if (shouldWrap)
                    q.OpenSubclause();

                for (int i = 0; i < be.Args.Count; i++)
                {
                    if (i > 0)
                    {
                        if (be.Boolop == BoolExprType.AndExpr)
                            q.AndAlso();
                        else
                            q.OrElse();
                    }

                    TranslateWhereClause(q, be.Args[i], fromAlias, be.Boolop);
                }

                if (shouldWrap)
                    q.CloseSubclause();

                return;
            }

            // AExpr: BETWEEN
            if (whereNode.AExpr is { Kind: A_Expr_Kind.AexprBetween } betweenExpr)
            {
                var leftField = ExtractFieldName(betweenExpr.Lexpr, fromAlias);
                if (string.IsNullOrEmpty(leftField))
                    throw new NotSupportedException("Unsupported BETWEEN (missing field)");

                if (betweenExpr.Rexpr?.List?.Items == null || betweenExpr.Rexpr.List.Items.Count != 2)
                    throw new NotSupportedException("Unsupported BETWEEN (expected two bounds)");

                var lower = ExtractValue(betweenExpr.Rexpr.List.Items[0]);
                var upper = ExtractValue(betweenExpr.Rexpr.List.Items[1]);
                if (lower is null || upper is null)
                    throw new NotSupportedException("Unsupported BETWEEN (missing bounds)");

                q.WhereBetween(leftField, lower, upper);
                return;
            }

            // AExpr: IN
            if (whereNode.AExpr is { Kind: A_Expr_Kind.AexprIn } inExpr)
            {
                var leftField = ExtractFieldName(inExpr.Lexpr, fromAlias);
                if (string.IsNullOrEmpty(leftField))
                    throw new NotSupportedException("Unsupported IN (missing field)");

                if (inExpr.Rexpr?.List?.Items == null || inExpr.Rexpr.List.Items.Count == 0)
                    throw new NotSupportedException("Unsupported IN (expected 1+ values)");

                var items = inExpr.Rexpr.List.Items;
                if (items.Count == 1 && items[0]?.List?.Items != null)
                    items = items[0].List.Items;

                if (items.Count == 0)
                    throw new NotSupportedException("Unsupported IN (expected 1+ values)");

                var values = new List<object>(capacity: items.Count);
                for (int i = 0; i < items.Count; i++)
                {
                    var v = ExtractValue(items[i]);
                    if (v is null)
                        throw new NotSupportedException("Unsupported IN (null value)");
                    values.Add(v);
                }

                q.WhereIn(leftField, values);
                return;
            }

            // AExpr: simple binary comparisons
            if (whereNode.AExpr is { Kind: A_Expr_Kind.AexprOp, Name.Count: > 0 } expr)
            {
                var operatorNode = expr.Name[0];
                var operatorName = GetStringValue(operatorNode) ?? "";
                var leftField = ExtractFieldName(expr.Lexpr, fromAlias);
                var rightValue = ExtractValue(expr.Rexpr);

                if (string.IsNullOrEmpty(leftField) || rightValue is null)
                    throw new NotSupportedException("Unsupported WHERE expression (missing field/value)");

                switch (operatorName)
                {
                    case "=":
                        q.WhereEquals(leftField, rightValue);
                        return;
                    case ">":
                        q.WhereGreaterThan(leftField, rightValue);
                        return;
                    case "<":
                        q.WhereLessThan(leftField, rightValue);
                        return;
                    case ">=":
                        q.WhereGreaterThanOrEqual(leftField, rightValue);
                        return;
                    case "<=":
                        q.WhereLessThanOrEqual(leftField, rightValue);
                        return;
                    case "!=":
                    case "<>":
                        q.WhereNotEquals(leftField, rightValue);
                        return;
                    default:
                        throw new NotSupportedException($"Unsupported operator '{operatorName}'");
                }
            }

            // NullTest: IS NULL / IS NOT NULL
            if (whereNode.NullTest != null)
            {
                var nt = whereNode.NullTest;

                var field = ExtractFieldName(nt.Arg, fromAlias);
                if (string.IsNullOrEmpty(field))
                    throw new NotSupportedException("Unsupported NULL test (missing field)");

                switch (nt.Nulltesttype)
                {
                    case NullTestType.IsNull:
                        q.WhereEquals(field, null);
                        return;

                    case NullTestType.IsNotNull:
                        q.WhereNotEquals(field, null);
                        return;

                    default:
                        throw new NotSupportedException($"Unsupported NULL test type: {nt.Nulltesttype}");
                }
            }

            throw new NotSupportedException("Unsupported WHERE clause node");
        }

        private static void TranslateOrderBy(AsyncDocumentQuery<JObject> q, Google.Protobuf.Collections.RepeatedField<Node> sortClause, string fromAlias)
        {
            foreach (var sortNode in sortClause)
            {
                if (sortNode.SortBy == null)
                    continue;

                var sortBy = sortNode.SortBy;
                var fieldName = ExtractFieldName(sortBy.Node, fromAlias);

                if (string.IsNullOrEmpty(fieldName))
                    continue;

                if (sortBy.SortbyDir == SortByDir.SortbyDesc)
                    q.OrderByDescending(fieldName);
                else
                    q.OrderBy(fieldName);
            }
        }

        private static int? TranslateLimit(Node limitNode)
        {
            if (limitNode.AConst?.Ival != null)
                return (int)limitNode.AConst.Ival.Ival;

            return null;
        }

        private static string ExtractFieldName(Node node, string fromAlias = null)
        {
            if (node?.ColumnRef != null && node.ColumnRef.Fields != null)
            {
                var fields = node.ColumnRef.Fields;
                if (fields.Count > 0)
                {
                    // If any segment is null, we can't reliably build a dotted field path.
                    // We soft-fail to allow falling back to other query handlers.
                    if (fields.Any(f => GetStringValue(f) == null))
                        throw new NotSupportedException("Unsupported field reference");

                    var field = string.Join('.', fields.Select(GetStringValue));
                    return StripAliasPrefix(field, fromAlias);
                }
            }

            return null;
        }

        private static string StripAliasPrefix(string field, string fromAlias)
        {
            if (string.IsNullOrWhiteSpace(field) || string.IsNullOrWhiteSpace(fromAlias))
                return field;

            var prefix = fromAlias + ".";
            if (field.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return field[prefix.Length..];

            return field;
        }

        private static object ExtractValue(Node node)
        {
            if (node?.AConst != null)
            {
                var constant = node.AConst;

                if (constant.Sval != null && string.IsNullOrEmpty(constant.Sval.Sval) == false)
                    return constant.Sval.Sval;

                if (constant.Ival != null)
                    return constant.Ival.Ival;

                if (constant.Fval != null && string.IsNullOrEmpty(constant.Fval.Fval) == false)
                {
                    // In pgsqlparser, float constants come as strings.
                    if (double.TryParse(constant.Fval.Fval, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                        return d;

                    return constant.Fval.Fval;
                }

                if (constant.Boolval != null)
                    return constant.Boolval.Boolval;
            }

            return null;
        }

        private static string GetStringValue(Node node)
        {
            return node.String?.Sval;
        }
    }
}

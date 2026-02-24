using System;
using System.Collections.Generic;
using PgSqlParser;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public sealed class PowerBIDirectQuery : PowerBIRqlQuery
    {
        public PowerBIDirectQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, Dictionary<string, ReplaceColumnValue> replaces = null, int? limit = null)
            : base(queryString, parametersDataTypes, documentDatabase, replaces, limit)
        {
        }

        protected override bool IncludeDocumentIdColumn => false;

        protected override bool IncludePowerBIJsonColumn => false;

        protected override DynamicJsonValue BeforeRow(BlittableJsonReaderObject jsonResult, short? jsonIndex)
        {
            return null;
        }

        protected override void AfterRow(BlittableJsonReaderObject jsonResult, ReadOnlyMemory<byte>?[] row, short? jsonIndex)
        {
        }

        private sealed record DirectQueryShape(
            List<string> ProjectionCols,
            List<string> GroupByCols,
            List<string> OrderByCols,
            List<bool> OrderByDescFlags,
            int Limit);

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out PgQuery pgQuery)
        {
            pgQuery = null;

            if (string.IsNullOrWhiteSpace(queryText))
                return false;

            try
            {
                // Very conservative handler for the PowerBI DirectQuery wrapper shape.
                // On any mismatch, return false so the other PowerBI parsers can handle it.
                var sql = queryText;

                if (TryExtractInnermostRql(sql, out var innerRql, out var innerStart, out var innerEnd) == false)
                    return false;

                // Replace the inner RQL (which PgSqlParser cannot parse) with a trivial SQL subquery.
                // Keep parentheses and surrounding SQL intact.
                var sanitizedSql = sql[..innerStart] + "select 1" + sql[innerEnd..];

                var parseResult = Parser.Parse(sanitizedSql);
                if (parseResult.IsSuccess == false || parseResult.Value == null)
                    return false;

                if (parseResult.Value.Stmts == null || parseResult.Value.Stmts.Count != 1)
                    return false;

                var stmt = parseResult.Value.Stmts[0];
                var selectStmt = stmt?.Stmt?.SelectStmt;
                if (selectStmt == null)
                    return false;

                if (TryExtractDirectQueryShape(selectStmt, out var shape) == false)
                    return false;

                if (IsValidRqlSelect(innerRql) == false)
                    return false;

                var rewrittenRql = RewriteRqlProjection(
                    innerRql,
                    projectionCols: shape.ProjectionCols,
                    shape.OrderByCols,
                    orderByDescFlags: shape.OrderByDescFlags,
                    shape.Limit);
                if (rewrittenRql == null)
                    return false;

                if (IsValidRqlSelect(rewrittenRql) == false)
                    return false;

                pgQuery = new PowerBIDirectQuery(rewrittenRql, parametersDataTypes, documentDatabase, replaces: null, limit: null);
                return true;
            }
            catch
            {
                pgQuery = null;
                return false;
            }

            static bool TryExtractOuterProjectedColumns(SelectStmt selectStmt, out List<string> cols)
            {
                cols = null;

                if (selectStmt?.TargetList == null || selectStmt.TargetList.Count == 0)
                    return false;

                cols = new List<string>(capacity: selectStmt.TargetList.Count);
                foreach (var t in selectStmt.TargetList)
                {
                    var resTarget = t?.ResTarget;
                    if (resTarget == null)
                        return false;

                    var colRef = resTarget.Val?.ColumnRef;
                    if (colRef == null)
                        return false;

                    if (TryExtractOuterUnderscoreQualifiedColumn(colRef, out var colName) == false)
                        return false;

                    cols.Add(colName);
                }

                return true;
            }

            static bool TryExtractInnerGroupBySelect(SelectStmt outerSelectStmt, out SelectStmt groupBySelect)
            {
                groupBySelect = null;

                if (outerSelectStmt?.FromClause is not { Count: 1 })
                    return false;

                var rss = outerSelectStmt.FromClause[0]?.RangeSubselect;
                if (rss == null)
                    return false;

                var aliasName = rss.Alias?.Aliasname;
                if (string.IsNullOrWhiteSpace(aliasName) || string.Equals(aliasName, "_", StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                groupBySelect = rss.Subquery?.SelectStmt;
                return groupBySelect != null;
            }

            static bool TryExtractGroupByColumns(SelectStmt selectStmt, out List<string> cols)
            {
                cols = null;

                if (selectStmt?.GroupClause == null || selectStmt.GroupClause.Count == 0)
                    return false;

                cols = new List<string>(capacity: selectStmt.GroupClause.Count);
                foreach (var node in selectStmt.GroupClause)
                {
                    var colRef = node?.ColumnRef;
                    if (colRef == null)
                        return false;

                    var colName = TryExtractLastIdentifierSegment(colRef);
                    if (string.IsNullOrWhiteSpace(colName))
                        return false;

                    cols.Add(colName);
                }

                return true;
            }

            static bool TryExtractOrderByColumns(SelectStmt selectStmt, out List<string> cols, out List<bool> descFlags)
            {
                cols = null;
                descFlags = null;

                if (selectStmt?.SortClause == null || selectStmt.SortClause.Count == 0)
                    return false;

                cols = new List<string>(capacity: selectStmt.SortClause.Count);
                descFlags = new List<bool>(capacity: selectStmt.SortClause.Count);

                foreach (var sortNode in selectStmt.SortClause)
                {
                    var sortBy = sortNode?.SortBy;
                    if (sortBy == null)
                        return false;

                    var colRef = sortBy.Node?.ColumnRef;
                    if (colRef == null)
                        return false;

                    if (TryExtractOuterUnderscoreQualifiedColumn(colRef, out var colName) == false)
                        return false;

                    cols.Add(colName);
                    descFlags.Add(sortBy.SortbyDir == SortByDir.SortbyDesc);
                }

                return true;
            }

            static bool TryExtractLimit(SelectStmt selectStmt, out int limit)
            {
                limit = 0;

                if (selectStmt?.LimitCount?.AConst?.Ival == null)
                    return false;

                limit = (int)selectStmt.LimitCount.AConst.Ival.Ival;
                return true;
            }

            static bool TryExtractDirectQueryShape(SelectStmt selectStmt, out DirectQueryShape shape)
            {
                shape = null;

                if (TryExtractOuterProjectedColumns(selectStmt, out var cols) == false)
                    return false;

                if (cols.Count == 0)
                    return false;

                if (TryExtractInnerGroupBySelect(selectStmt, out var groupBySelect) == false)
                    return false;

                if (TryExtractGroupByColumns(groupBySelect, out var groupByCols) == false)
                    return false;

                if (groupByCols.Count == 0)
                    return false;

                if (IsSubsetIgnoreCase(cols, groupByCols) == false)
                    return false;

                if (TryExtractOrderByColumns(selectStmt, out var orderByCols, out var orderDescFlags) == false)
                    return false;

                if (orderByCols.Count == 0)
                    return false;

                if (IsSubsetIgnoreCase(orderByCols, cols) == false)
                    return false;

                if (TryExtractLimit(selectStmt, out var limit) == false)
                    return false;

                shape = new DirectQueryShape(cols, groupByCols, orderByCols, orderDescFlags, limit);
                return true;
            }

            static bool IsValidRqlSelect(string rql)
            {
                try
                {
                    QueryMetadata.ParseQuery(rql, QueryType.Select);
                    return true;
                }
                catch
                {
                    return false;
                }
            }

            static bool IsSubsetIgnoreCase(IReadOnlyList<string> subset, IReadOnlyList<string> superset)
            {
                if (subset == null || superset == null)
                    return false;

                var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var s in superset)
                {
                    if (string.IsNullOrWhiteSpace(s))
                        continue;

                    set.Add(s);
                }

                foreach (var s in subset)
                {
                    if (string.IsNullOrWhiteSpace(s))
                        return false;

                    if (set.Contains(s) == false)
                        return false;
                }

                return true;
            }

            static string TryExtractLastIdentifierSegment(ColumnRef colRef)
            {
                if (colRef?.Fields == null || colRef.Fields.Count == 0)
                    return null;

                var last = colRef.Fields[^1];
                var name = last?.String?.Sval;
                return string.IsNullOrWhiteSpace(name) ? null : name;
            }

            static bool TryExtractOuterUnderscoreQualifiedColumn(ColumnRef colRef, out string colName)
            {
                colName = null;

                if (colRef?.Fields == null || colRef.Fields.Count < 2)
                    return false;

                var first = colRef.Fields[0]?.String?.Sval;
                if (string.Equals(first, "_", StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                colName = TryExtractLastIdentifierSegment(colRef);
                return string.IsNullOrWhiteSpace(colName) == false;
            }

            static bool TryExtractInnermostRql(string sql, out string innerRql, out int innerStart, out int innerEnd)
            {
                innerRql = null;
                innerStart = 0;
                innerEnd = 0;

                const string endToken = ") \"$Table\"";

                int end = -1;
                int nextSearch = 0;
                while (true)
                {
                    var idx = sql.IndexOf(endToken, nextSearch, StringComparison.OrdinalIgnoreCase);
                    if (idx == -1)
                        break;

                    if (end != -1)
                        return false; // ambiguous

                    end = idx;
                    nextSearch = idx + endToken.Length;
                }

                if (end == -1)
                    return false;

                // Find the matching '(' for this ')', accounting for parentheses inside the RQL.
                // We then validate it is preceded by `from` (ignoring whitespace).
                int depth = 0;
                int open = -1;
                for (int i = end - 1; i >= 0; i--)
                {
                    var ch = sql[i];
                    if (ch == ')')
                    {
                        depth++;
                        continue;
                    }

                    if (ch != '(')
                        continue;

                    if (depth > 0)
                    {
                        depth--;
                        continue;
                    }

                    open = i;
                    break;
                }

                if (open == -1)
                    return false;

                // Ensure this paren belongs to a `from (` wrapper.
                int j = open - 1;
                while (j >= 0 && char.IsWhiteSpace(sql[j]))
                    j--;

                if (j < 3)
                    return false;

                // Expect: ... from (
                var fromStart = j - 3;
                if (fromStart < 0)
                    return false;

                if (string.Equals(sql.Substring(fromStart, 4), "from", StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                // We slice the inner RQL without trimming to preserve exact indices for sanitization.
                innerStart = open + 1;
                innerEnd = end;
                if (innerStart >= innerEnd)
                    return false;

                var trimmedStart = innerStart;
                var trimmedEnd = innerEnd;
                while (trimmedStart < trimmedEnd && char.IsWhiteSpace(sql[trimmedStart]))
                    trimmedStart++;
                while (trimmedEnd > trimmedStart && char.IsWhiteSpace(sql[trimmedEnd - 1]))
                    trimmedEnd--;

                if (trimmedStart >= trimmedEnd)
                    return false;

                innerRql = sql.Substring(trimmedStart, trimmedEnd - trimmedStart);
                return string.IsNullOrWhiteSpace(innerRql) == false;
            }

            static string RewriteRqlProjection(string innerRql, IReadOnlyList<string> projectionCols, IReadOnlyList<string> orderByCols, IReadOnlyList<bool> orderByDescFlags, int limit)
            {
                if (string.IsNullOrWhiteSpace(innerRql))
                    return null;

                if (projectionCols == null || projectionCols.Count == 0)
                    return null;

                if (orderByCols == null || orderByCols.Count == 0)
                    return null;

                if (orderByDescFlags == null || orderByDescFlags.Count != orderByCols.Count)
                    return null;

                Documents.Queries.AST.Query q;
                try
                {
                    q = QueryMetadata.ParseQuery(innerRql, QueryType.Select);
                }
                catch
                {
                    return null;
                }

                if (q == null)
                    return null;

                Dictionary<string, string> projectionExprs = null;
                if (q.SelectFunctionBody.FunctionText != null)
                {
                    // Use the already-parsed select function body when available to preserve expressions like `name(e)`.
                    projectionExprs = TryExtractProjectionExpressions(q.SelectFunctionBody.FunctionText.AsSpan());
                }

                var prefixQuery = q.ShallowCopy();
                prefixQuery.IsDistinct = false;
                prefixQuery.Filter = null;
                prefixQuery.FilterLimit = null;
                prefixQuery.OrderBy = null;
                prefixQuery.Select = null;
                prefixQuery.SelectFunctionBody = default;
                prefixQuery.Limit = null;
                prefixQuery.Offset = null;

                var prefix = prefixQuery.ToString();
                if (string.IsNullOrWhiteSpace(prefix))
                    return null;

                prefix = prefix.TrimEnd();
                prefix = prefix.Replace("\r\n", "\n", StringComparison.Ordinal);

                var orderByParts = new List<string>(capacity: orderByCols.Count);
                for (int i = 0; i < orderByCols.Count; i++)
                {
                    var colName = orderByCols[i];
                    if (string.IsNullOrWhiteSpace(colName))
                        return null;

                    var id = FormatRqlIdentifier(colName);
                    if (id == null)
                        return null;

                    orderByParts.Add(id + (orderByDescFlags[i] ? " desc" : string.Empty));
                }

                var selectParts = new List<string>(capacity: projectionCols.Count);
                for (int i = 0; i < projectionCols.Count; i++)
                {
                    var colName = projectionCols[i];
                    if (string.IsNullOrWhiteSpace(colName))
                        return null;

                    var id = FormatRqlIdentifier(colName);
                    if (id == null)
                        return null;

                    var selectField = FormatRqlObjectFieldIdentifier(colName);
                    if (selectField == null)
                        return null;

                    var expr = id;
                    if (projectionExprs != null && projectionExprs.TryGetValue(colName, out var extracted) && string.IsNullOrWhiteSpace(extracted) == false)
                        expr = extracted;

                    selectParts.Add($"{selectField}: {expr}");
                }

                const string nl = "\n";
                return prefix + nl +
                       $"order by {string.Join(", ", orderByParts)}" + nl +
                       $"select distinct {{ {string.Join(", ", selectParts)} }}" + nl +
                       $"limit 0, {limit}";
            }


            /*
            Legacy string-based fallback (kept for later; not used in runtime logic during demo/review)

            static string RewriteRqlProjection_StringFallback(string innerRql, IReadOnlyList<string> projectionCols, IReadOnlyList<string> orderByCols, IReadOnlyList<bool> orderByDescFlags, int limit)
            {
                if (string.IsNullOrWhiteSpace(innerRql))
                    return null;

                if (projectionCols == null || projectionCols.Count == 0)
                    return null;

                if (orderByCols == null || orderByCols.Count == 0)
                    return null;

                if (orderByDescFlags == null || orderByDescFlags.Count != orderByCols.Count)
                    return null;

                var idxSelect = innerRql.LastIndexOf("select", StringComparison.OrdinalIgnoreCase);
                if (idxSelect == -1)
                    return null;

                var prefix = innerRql.Substring(0, idxSelect).TrimEnd();
                if (string.IsNullOrWhiteSpace(prefix))
                    return null;

                var projectionExprs = TryExtractProjectionExpressions(innerRql.AsSpan(idxSelect));

                var orderByParts = new List<string>(capacity: orderByCols.Count);
                var selectParts = new List<string>(capacity: projectionCols.Count);

                for (int i = 0; i < projectionCols.Count; i++)
                {
                    var colName = projectionCols[i];
                    if (string.IsNullOrWhiteSpace(colName))
                        return null;

                    var id = FormatRqlIdentifier(colName);
                    if (id == null)
                        return null;

                    var selectField = FormatRqlObjectFieldIdentifier(colName);
                    if (selectField == null)
                        return null;

                    var expr = id;
                    if (projectionExprs != null && projectionExprs.TryGetValue(colName, out var extracted) && string.IsNullOrWhiteSpace(extracted) == false)
                        expr = extracted;

                    selectParts.Add($"{selectField}: {expr}");
                }

                for (int i = 0; i < orderByCols.Count; i++)
                {
                    var colName = orderByCols[i];
                    if (string.IsNullOrWhiteSpace(colName))
                        return null;

                    var id = FormatRqlIdentifier(colName);
                    if (id == null)
                        return null;

                    orderByParts.Add(id + (orderByDescFlags[i] ? " desc" : string.Empty));
                }

                const string nl = "\n";
                return prefix + nl +
                       $"order by {string.Join(", ", orderByParts)}" + nl +
                       $"select distinct {{ {string.Join(", ", selectParts)} }}" + nl +
                       $"limit 0, {limit}";
            }
            */

            static Dictionary<string, string> TryExtractProjectionExpressions(ReadOnlySpan<char> selectClause)
            {
                // Attempt to extract `<field>: <expr>` pairs from `select { ... }` (or just `{ ... }`) in the original inner RQL.
                // On any mismatch, return null and the caller will fall back to using the column identifier as the expression.

                var idxOpen = -1;

                var idxSelect = selectClause.IndexOf("select", StringComparison.OrdinalIgnoreCase);
                if (idxSelect != -1)
                {
                    var idxBrace = selectClause.Slice(idxSelect).IndexOf('{');
                    if (idxBrace != -1)
                        idxOpen = idxSelect + idxBrace;
                }

                if (idxOpen == -1)
                {
                    var idxReturn = selectClause.IndexOf("return", StringComparison.OrdinalIgnoreCase);
                    if (idxReturn != -1)
                    {
                        var idxBrace = selectClause.Slice(idxReturn).IndexOf('{');
                        if (idxBrace != -1)
                            idxOpen = idxReturn + idxBrace;
                    }
                }

                if (idxOpen == -1)
                    idxOpen = selectClause.IndexOf('{');
                if (idxOpen == -1)
                    return null;

                var idxClose = FindMatchingBrace(selectClause, idxOpen);
                if (idxClose == -1)
                    return null;

                var body = selectClause.Slice(idxOpen + 1, idxClose - idxOpen - 1);
                int pos = 0;

                var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                while (pos < body.Length)
                {
                    SkipWs(body, ref pos);
                    if (pos >= body.Length)
                        break;

                    if (body[pos] == ',')
                    {
                        pos++;
                        continue;
                    }

                    if (TryReadFieldName(body, ref pos, out var fieldName) == false)
                        return null;

                    if (string.IsNullOrWhiteSpace(fieldName))
                        return null;

                    SkipWs(body, ref pos);
                    if (pos >= body.Length || body[pos] != ':')
                        return null;

                    pos++; // ':'
                    SkipWs(body, ref pos);

                    var exprStart = pos;
                    if (TryScanExpression(body, ref pos, out var exprEnd) == false)
                        return null;

                    var expr = body.Slice(exprStart, exprEnd - exprStart).ToString().Trim();
                    if (string.IsNullOrWhiteSpace(expr))
                        return null;

                    dict[fieldName] = expr;

                    SkipWs(body, ref pos);
                    if (pos < body.Length && body[pos] == ',')
                        pos++;
                }

                return dict.Count == 0 ? null : dict;

                static int FindMatchingBrace(ReadOnlySpan<char> s, int openIndex)
                {
                    int depth = 0;
                    bool inString = false;
                    char stringQuote = '\0';

                    for (int i = openIndex; i < s.Length; i++)
                    {
                        var ch = s[i];

                        if (inString)
                        {
                            if (ch == '\\')
                            {
                                i++;
                                continue;
                            }

                            if (ch == stringQuote)
                                inString = false;

                            continue;
                        }

                        if (ch == '\'' || ch == '"')
                        {
                            inString = true;
                            stringQuote = ch;
                            continue;
                        }

                        if (ch == '{')
                        {
                            depth++;
                            continue;
                        }

                        if (ch == '}')
                        {
                            depth--;
                            if (depth == 0)
                                return i;
                        }
                    }

                    return -1;
                }

                static void SkipWs(ReadOnlySpan<char> s, ref int i)
                {
                    while (i < s.Length && char.IsWhiteSpace(s[i]))
                        i++;
                }

                static bool TryReadFieldName(ReadOnlySpan<char> s, ref int i, out string field)
                {
                    field = null;
                    if (i >= s.Length)
                        return false;

                    if (s[i] == '"' || s[i] == '\'')
                    {
                        var quote = s[i++];
                        int start = i;
                        while (i < s.Length)
                        {
                            var ch = s[i];
                            if (ch == '\\')
                            {
                                i += 2;
                                continue;
                            }

                            if (ch == quote)
                            {
                                field = s.Slice(start, i - start).ToString();
                                i++;
                                return true;
                            }

                            i++;
                        }

                        return false;
                    }

                    int nameStart = i;
                    while (i < s.Length)
                    {
                        var ch = s[i];
                        if (char.IsWhiteSpace(ch) || ch == ':' || ch == ',')
                            break;
                        i++;
                    }

                    if (i == nameStart)
                        return false;

                    field = s.Slice(nameStart, i - nameStart).ToString();
                    return true;
                }

                static bool TryScanExpression(ReadOnlySpan<char> s, ref int i, out int exprEnd)
                {
                    exprEnd = i;

                    int depth = 0;
                    bool inString = false;
                    char stringQuote = '\0';

                    for (; i < s.Length; i++)
                    {
                        var ch = s[i];

                        if (inString)
                        {
                            if (ch == '\\')
                            {
                                i++;
                                continue;
                            }

                            if (ch == stringQuote)
                                inString = false;

                            continue;
                        }

                        if (ch == '\'' || ch == '"')
                        {
                            inString = true;
                            stringQuote = ch;
                            continue;
                        }

                        if (ch is '(' or '[' or '{')
                        {
                            depth++;
                            continue;
                        }

                        if (ch is ')' or ']' or '}')
                        {
                            if (depth > 0)
                            {
                                depth--;
                                continue;
                            }
                        }

                        if (depth == 0 && ch == ',')
                        {
                            exprEnd = i;
                            return true;
                        }
                    }

                    exprEnd = s.Length;
                    return true;
                }
            }

            static string FormatRqlIdentifier(string identifier)
            {
                if (string.IsNullOrWhiteSpace(identifier))
                    return null;

                bool IsAsciiLetter(char ch) => (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
                bool IsAsciiDigit(char ch) => ch >= '0' && ch <= '9';
                bool IsStart(char ch) => IsAsciiLetter(ch) || ch == '_';
                bool IsPart(char ch) => IsStart(ch) || IsAsciiDigit(ch);

                if (IsStart(identifier[0]) == false)
                    return Escape(identifier);

                for (int i = 1; i < identifier.Length; i++)
                {
                    if (IsPart(identifier[i]) == false)
                        return Escape(identifier);
                }

                return identifier;

                static string Escape(string raw)
                {
                    var escaped = raw.Replace("\"", "\\\"", StringComparison.Ordinal);
                    return $"[\"{escaped}\"]";
                }
            }

            static string FormatRqlObjectFieldIdentifier(string identifier)
            {
                if (string.IsNullOrWhiteSpace(identifier))
                    return null;

                bool IsAsciiLetter(char ch) => (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
                bool IsAsciiDigit(char ch) => ch >= '0' && ch <= '9';
                bool IsStart(char ch) => IsAsciiLetter(ch) || ch == '_';
                bool IsPart(char ch) => IsStart(ch) || IsAsciiDigit(ch);

                if (IsStart(identifier[0]) == false)
                    return Quote(identifier);

                for (int i = 1; i < identifier.Length; i++)
                {
                    if (IsPart(identifier[i]) == false)
                        return Quote(identifier);
                }

                return identifier;

                static string Quote(string raw)
                {
                    var escaped = raw.Replace("\"", "\\\"", StringComparison.Ordinal);
                    return "\"" + escaped + "\"";
                }
            }
        }
    }
}

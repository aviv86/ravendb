using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Exceptions;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class Query
    {
        public bool IsDistinct;
        public GraphQuery GraphQuery;
        public QueryExpression Where;
        public FromClause From;
        public List<(QueryExpression Expression, StringSegment? Alias)> Select;
        public List<(QueryExpression Expression, StringSegment? Alias)> Load;
        public List<QueryExpression> Include;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<(QueryExpression Expression, StringSegment? Alias)> GroupBy;

        public Dictionary<string, DeclaredFunction> DeclaredFunctions;

        public string QueryText;
        public (string FunctionText, Esprima.Ast.Program Program) SelectFunctionBody;
        public string UpdateBody;
        public ValueExpression Offset;
        public ValueExpression Limit;

        public bool TryAddFunction(DeclaredFunction func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);

            return DeclaredFunctions.TryAdd(func.Name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).Visit(this);
            return sb.ToString();
        }

        public void TryAddWithClause(Query query, StringSegment alias, bool implicitAlias)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();                
            }

            if (GraphQuery.WithDocumentQueries.TryGetValue(alias, out var existing))
            {
                if (query.From.From.Compound.Count == 0)
                    return; // reusing an alias defined explicitly before

                if(existing.withQuery.From.From.Compound.Count == 0)
                {
                    // using an alias that is defined _later_ in the query
                    GraphQuery.WithDocumentQueries[alias] = (implicitAlias, query);
                    return;
                }

                throw new InvalidQueryException($"Alias {alias} is already in use on a different 'With' clause", QueryText);
            }

            GraphQuery.WithDocumentQueries.Add(alias, (implicitAlias, query));
        }

        public void TryAddWithEdgePredicates(WithEdgesExpression expr, StringSegment alias)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();               
            }

            if (GraphQuery.WithEdgePredicates.ContainsKey(alias))
            {
                if (expr.Path.Compound.Count == 0 && expr.OrderBy == null && expr.Where == null)
                    return;

                throw new InvalidQueryException($"Allias {alias} is already in use on a diffrent 'With' clause",
                    QueryText, null);
            }

            GraphQuery.WithEdgePredicates.Add(alias, expr);
        }
    }

    public class DeclaredFunction
    {
        public string Name;
        public string FunctionText;
        public Esprima.Ast.Program Program;
        public FunctionType Type;

        public enum FunctionType
        {
            JavaScript,
            TimeSeries
        }
    }
}

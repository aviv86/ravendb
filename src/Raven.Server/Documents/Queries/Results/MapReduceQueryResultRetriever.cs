﻿using System;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : StoredValueQueryResultRetriever
    {
        public MapReduceQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
            : base(Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName, database, query, queryTimings, documentsStorage, context, searchEngineType, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
        }
    }

    public abstract class StoredValueQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly string _storedValueFieldName;
        private readonly JsonOperationContext _context;
        private QueryTimingsScope _storageScope;

        protected StoredValueQueryResultRetriever(string storedValueFieldName, DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
            : base(database, query, queryTimings, searchEngineType, fieldsToFetch, documentsStorage, context, true, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
            if (storedValueFieldName == null)
                throw new ArgumentNullException(nameof(storedValueFieldName));

            _storedValueFieldName = storedValueFieldName;
            _context = context;
        }

        protected override void ValidateFieldsToFetch(FieldsToFetch fieldsToFetch)
        {
            base.ValidateFieldsToFetch(fieldsToFetch);

            if (fieldsToFetch.Projection.MustExtractFromDocument)
                throw new InvalidQueryException($"Invalid projection behavior '{_query.ProjectionBehavior}'. You can only extract values from index.", _query.Query, _query.QueryParameters);
        }

        protected override Document LoadDocument(string id)
        {
            if (DocumentsStorage != null &&
                _context is DocumentsOperationContext ctx)
                return DocumentsStorage.Get(ctx, id);
            // can happen during some debug endpoints that should never load a document
            return null;
        }

        protected override long? GetCounter(string docId, string name)
        {
            if (DocumentsStorage != null &&
                _context is DocumentsOperationContext ctx)
                return DocumentsStorage.CountersStorage.GetCounterValue(ctx, docId, name)?.Value;
            return null;
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            if (DocumentsStorage == null || !(_context is DocumentsOperationContext ctx))
                return null;

            var djv = new DynamicJsonValue();

            foreach (var partialValue in DocumentsStorage.CountersStorage.GetCounterPartialValues(ctx, docId, name))
            {
                djv[partialValue.ChangeVector] = partialValue.PartialValue;
            }

            return djv;
        }

        protected override unsafe Document DirectGet(Lucene.Net.Documents.Document input, string id, DocumentFields fields, IState state)
        {
            var storedValue = input.GetField(_storedValueFieldName).GetBinaryValue(state);

            var allocation = _context.GetMemory(storedValue.Length);

            UnmanagedWriteBuffer buffer = new UnmanagedWriteBuffer(_context, allocation);
            buffer.Write(storedValue, 0, storedValue.Length);

            var result = new BlittableJsonReaderObject(allocation.Address, storedValue.Length, _context, buffer);

            return new Document
            {
                Data = result
            };
        }

        public override Document Get(ref RetrieverInput retrieverInput)
        {
            if (FieldsToFetch.IsProjection)
                return GetProjection( ref retrieverInput, null);

            using (_storageScope = _storageScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Storage)))
            {
                var doc = DirectGet(retrieverInput.LuceneDocument, null, DocumentFields.All, retrieverInput.State);

                FinishDocumentSetup(doc, retrieverInput.Score);

                return doc;
            }
        }

        public override bool TryGetKey(ref RetrieverInput retrieverInput, out string key)
        {
            key = null;
            return false;
        }
    }
}

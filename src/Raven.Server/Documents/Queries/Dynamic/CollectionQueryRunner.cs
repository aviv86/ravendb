﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries.Dynamic
{
    public class CollectionQueryRunner : AbstractQueryRunner
    {
        public const string CollectionIndexPrefix = "collection/";

        public CollectionQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        public override Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var result = new DocumentQueryResult();

            if (documentsContext.Transaction == null || documentsContext.Transaction.Disposed)
                documentsContext.OpenReadTransaction();

            FillCountOfResultsAndIndexEtag(result, query.Metadata, documentsContext);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                if (result.ResultEtag == existingResultEtag)
                    return Task.FromResult(DocumentQueryResult.NotModifiedResult);
            }

            var collection = GetCollectionName(query.Metadata.CollectionName, out var indexName);

            using (QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                result.IndexName = indexName;

                ExecuteCollectionQuery(result, query, collection, documentsContext, pulseReadingTransaction: false, token.Token);

                return Task.FromResult(result);
            }
        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamQueryResultWriter<Document> writer,
            OperationCancelToken token)
        {
            var result = new StreamDocumentQueryResult(response, writer, token);
            documentsContext.OpenReadTransaction();

            FillCountOfResultsAndIndexEtag(result, query.Metadata, documentsContext);

            var collection = GetCollectionName(query.Metadata.CollectionName, out var indexName);

            using (QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                result.IndexName = indexName;

                ExecuteCollectionQuery(result, query, collection, documentsContext, pulseReadingTransaction: true, token.Token);

                result.Flush();

                return Task.CompletedTask;
            }
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response,
            IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so index entries aren't created underneath");
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var runner = new CollectionRunner(Database, context, query);

            return runner.ExecuteDelete(query.Metadata.CollectionName, query.Start, query.PageSize, new CollectionOperationOptions
            {
                MaxOpsPerSecond = options.MaxOpsPerSecond
            }, onProgress, token);
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var runner = new CollectionRunner(Database, context, query);

            return runner.ExecutePatch(query.Metadata.CollectionName, query.Start, query.PageSize, new CollectionOperationOptions
            {
                MaxOpsPerSecond = options.MaxOpsPerSecond
            }, patch, patchArgs, onProgress, token);
        }

        public override Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            throw new NotSupportedException("Collection query is handled directly by documents storage so suggestions aren't supported");
        }

        private void ExecuteCollectionQuery(QueryResultServerSide<Document> resultToFill, IndexQueryServerSide query, string collection, DocumentsOperationContext context, bool pulseReadingTransaction, CancellationToken cancellationToken)
        {
            using (var queryScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Query)))
            {
                QueryTimingsScope gatherScope = null;
                QueryTimingsScope fillScope = null;

                if (queryScope != null && query.Metadata.Includes?.Length > 0)
                {
                    var includesScope = queryScope.For(nameof(QueryTimingsScope.Names.Includes), start: false);
                    gatherScope = includesScope.For(nameof(QueryTimingsScope.Names.Gather), start: false);
                    fillScope = includesScope.For(nameof(QueryTimingsScope.Names.Fill), start: false);
                }

                // we optimize for empty queries without sorting options, appending CollectionIndexPrefix to be able to distinguish index for collection vs. physical index
                resultToFill.IsStale = false;
                resultToFill.LastQueryTime = DateTime.MinValue;
                resultToFill.IndexTimestamp = DateTime.MinValue;
                resultToFill.IncludedPaths = query.Metadata.Includes;

                var fieldsToFetch = new FieldsToFetch(query, null);
                var includeDocumentsCommand = new IncludeDocumentsCommand(Database.DocumentsStorage, context, query.Metadata.Includes, fieldsToFetch.IsProjection);
                var totalResults = new Reference<int>();

                IEnumerator<Document> enumerator;

                if (pulseReadingTransaction == false)
                {
                    var documents = new CollectionQueryEnumerable(Database, Database.DocumentsStorage, fieldsToFetch, collection, query, queryScope, context, includeDocumentsCommand, totalResults);

                    enumerator = documents.GetEnumerator();
                }
                else
                {
                    enumerator = new PulsedTransactionEnumerator<Document, CollectionQueryResultsIterationState>(context,
                        state =>
                        {
                            query.Start = state.Start;
                            query.PageSize = state.Take;

                            var documents = new CollectionQueryEnumerable(Database, Database.DocumentsStorage, fieldsToFetch, collection, query, queryScope, context,
                                includeDocumentsCommand, totalResults);

                            return documents;
                        },
                        new CollectionQueryResultsIterationState(context, Database.Configuration.Databases.PulseReadTransactionLimit)
                        {
                            Start = query.Start, 
                            Take = query.PageSize
                        });
                }

                IncludeCountersCommand includeCountersCommand = null;
                IncludeTimeSeriesCommand includeTimeSeriesCommand = null;
                if (query.Metadata.CounterIncludes != null)
                {
                    includeCountersCommand = new IncludeCountersCommand(
                        Database,
                        context,
                        query.Metadata.CounterIncludes.Counters);
                }

                if (query.Metadata.TimeSeriesIncludes != null)
                {
                    includeTimeSeriesCommand = new IncludeTimeSeriesCommand(
                        Database, 
                        context, 
                        query.Metadata.TimeSeriesIncludes.TimeSeries);
                }

                try
                {
                    using (enumerator)
                    {
                        while (enumerator.MoveNext())
                        {
                            var document = enumerator.Current;

                            cancellationToken.ThrowIfCancellationRequested();

                            resultToFill.AddResult(document);

                            using (gatherScope?.Start())
                                includeDocumentsCommand.Gather(document);

                            includeCountersCommand?.Fill(document);

                            includeTimeSeriesCommand?.Fill(document);
                        }

                    }
                }
                catch (Exception e)
                {
                    if (resultToFill.SupportsExceptionHandling == false)
                        throw;

                    resultToFill.HandleException(e);
                }

                using (fillScope?.Start())
                    includeDocumentsCommand.Fill(resultToFill.Includes);

                if (includeCountersCommand != null)
                    resultToFill.AddCounterIncludes(includeCountersCommand);

                if (includeTimeSeriesCommand != null)
                    resultToFill.AddTimeSeriesIncludes(includeTimeSeriesCommand);

                resultToFill.TotalResults = (totalResults.Value == 0 && resultToFill.Results.Count != 0) ? -1 : totalResults.Value;

                if (query.Offset != null || query.Limit != null)
                {
                    if (resultToFill.TotalResults == -1)
                    {
                        resultToFill.CappedMaxResults = query.Limit ?? -1;
                    }
                    else
                    {
                        resultToFill.CappedMaxResults = Math.Min(
                            query.Limit ?? int.MaxValue,
                            resultToFill.TotalResults - (query.Offset ?? 0)
                        );    
                    }
                }
            }
        }

        private unsafe void FillCountOfResultsAndIndexEtag(QueryResultServerSide<Document> resultToFill, QueryMetadata query, DocumentsOperationContext context)
        {
            var bufferSize = 3;
            var hasCounters = query.HasCounterSelect || query.CounterIncludes != null;
            var hasTimeSeries = query.HasTimeSeriesSelect || query.HasTimeSeriesDeclarations || query.TimeSeriesIncludes != null;

            if (hasCounters)
                bufferSize++;
            if (hasTimeSeries)
                bufferSize++;
            if (query.HasCmpXchgSelect)
                bufferSize++;

            var collection = query.CollectionName;
            var buffer = stackalloc long[bufferSize];

            // If the query has include or load, it's too difficult to check the etags for just the included collections, 
            // it's easier to just show etag for all docs instead.
            if (collection == Constants.Documents.Collections.AllDocumentsCollection ||
                query.HasIncludeOrLoad)
            {
                var numberOfDocuments = Database.DocumentsStorage.GetNumberOfDocuments(context);
                buffer[0] = DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);
                buffer[1] = DocumentsStorage.ReadLastTombstoneEtag(context.Transaction.InnerTransaction);
                buffer[2] = numberOfDocuments;

                if (hasCounters)
                    buffer[3] = DocumentsStorage.ReadLastCountersEtag(context.Transaction.InnerTransaction);

                if (hasTimeSeries)
                    buffer[hasCounters ? 4 : 3] = DocumentsStorage.ReadLastTimeSeriesEtag(context.Transaction.InnerTransaction);

                resultToFill.TotalResults = (int)numberOfDocuments;
            }
            else
            {
                var collectionStats = Database.DocumentsStorage.GetCollection(collection, context);
                buffer[0] = Database.DocumentsStorage.GetLastDocumentEtag(context, collection);
                buffer[1] = Database.DocumentsStorage.GetLastTombstoneEtag(context, collection);
                buffer[2] = collectionStats.Count;

                if (hasCounters)
                    buffer[3] = Database.DocumentsStorage.CountersStorage.GetLastCounterEtag(context, collection);

                if (hasTimeSeries)
                    buffer[hasCounters ? 4 : 3] = Database.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(context, collection);

                resultToFill.TotalResults = (int)collectionStats.Count;
            }

            if (query.HasCmpXchgSelect)
            {
                using (context.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionContext))
                using (transactionContext.OpenReadTransaction())
                {
                    buffer[bufferSize - 1] = Database.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(transactionContext, Database.Name);
                }
            }

            resultToFill.ResultEtag = (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * (uint)bufferSize);
            resultToFill.NodeTag = Database.ServerStore.NodeTag;
        }

        private static string GetCollectionName(string collection, out string indexName)
        {
            if (string.IsNullOrEmpty(collection))
                collection = Constants.Documents.Collections.AllDocumentsCollection;

            indexName = collection == Constants.Documents.Collections.AllDocumentsCollection 
                ? "AllDocs" 
                : CollectionIndexPrefix + collection;

            return collection;
        }
    }
}

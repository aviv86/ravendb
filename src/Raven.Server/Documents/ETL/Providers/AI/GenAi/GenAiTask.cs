using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Extensions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.GenAi;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Utils;
using Encoding = System.Text.Encoding;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public sealed class GenAiTask : EtlProcess<GenAiItem, GenAiScriptResult, GenAiConfiguration, AiConnectionString,
    GenAiStatsScope, GenAiPerformanceOperation>
{
    public const string GenAiTaskTag = "Gen/AI";

    private const string TestDocumentId = "GenAi/TestDocument";
    private int _fallbackCounter = 0;
    private AbstractChatCompletionClient _chatCompletionClient;

    public GenAiTask(Transformation transformation, GenAiConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, GenAiTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();

        if (configuration.TestMode == false)
            _chatCompletionClient = GetClient();
    }

    private AbstractChatCompletionClient GetClient()
    {
        if (string.IsNullOrWhiteSpace(Configuration.JsonSchema))
            Configuration.JsonSchema = AbstractChatCompletionClient.GetSchemaFor(Configuration.SampleObject);

        var connectorType = Configuration.Connection.GetActiveProvider();
        AbstractChatCompletionClient client = connectorType switch
        {
            AiConnectorType.Ollama => new OllamaChatCompletionClient(Configuration, Database.ServerStore.ContextPool, AbstractChatCompletionClient.DefaultConventions),
            AiConnectorType.OpenAi => new OpenAiChatCompletionClient(Configuration, Database.ServerStore.ContextPool, AbstractChatCompletionClient.DefaultConventions),
            _ => throw new NotSupportedException(connectorType.ToString())
        };

        return client;
    }

    public override EtlType EtlType => EtlType.GenAi;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;

    protected override bool ShouldTrackAttachmentTombstones() => false;

    public override bool ShouldTrackDocumentTombstones() => false;

    protected override IEnumerator<GenAiItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToGenAiItems(docs, collection);
    }

    protected override IEnumerator<GenAiItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
        bool trackAttachments)
    {
        throw new NotSupportedException($"{nameof(ConvertTombstonesEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
        List<string> collections)
    {
        throw new NotSupportedException($"{nameof(ConvertAttachmentTombstonesEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertCountersEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override IEnumerator<GenAiItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesDeletedRangeEnumerator)} is not supported for {nameof(GenAiTask)}");
    }

    protected override EtlTransformer<GenAiItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new GenAiScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override string LoadFailureMessage =>
        $"Gen AI task '{Configuration.Name}' failed during model communication or update phase. Retrying in {FallbackTime}";

    protected override void EnterFallbackMode(Exception e, DateTime? lastErrorTime)
    {
        if (e is RateLimitException rateLimitException)
        {
            FallbackTime = rateLimitException.RetryAfter;
            return;
        }

        base.EnterFallbackMode(e, lastErrorTime);
    }

    protected override int LoadInternal(IEnumerable<GenAiScriptResult> items, DocumentsOperationContext context, GenAiStatsScope scope)
    {
        var results = PrepareItemsBeforeSendingToModel(items);
        if (results.Count is 0)
            return 0;

        var exceptions = SendToModel(results, context, scope);

        ApplyUpdateScript(context, results);

        if (exceptions?.Count > 0)
        {
            foreach (var e in exceptions)
            {
                if (e is RefusedToAnswerException or TooManyTokensException)
                    continue;

                throw e;
            }
        }

        return results.Count;
    }

    private List<Exception> SendToModel(List<GenAiResultItem> items, DocumentsOperationContext context, GenAiStatsScope scope)
    {
        using (var statsScope = scope.For(GenAiOperations.LoadToModel))
        {
            context.CloseTransaction();

            List<Task<(string Result, string Usage)>> tasks = [];

            foreach (var item in items)
            {
                statsScope.NumberOfContextObjects++;

                if (item.ContextOutput.IsCached)
                {
                    statsScope.TotalCachedContexts++;
                    continue; // no change, can skip
                }

                statsScope.TotalSentToModel++;

                string json = item.ContextOutput.Context.ToString();
                tasks.Add(_chatCompletionClient.CompleteAsync(Configuration.Prompt, json, Database.DatabaseShutdown));
            }

            try
            {
                // TODO: Yuck
                Task.WaitAll(tasks.OfType<Task>().ToArray());
            }
            catch
            {
                // we'll handle that later
            }

            return ProcessModelResults(items, context, tasks, statsScope);
        }
    }

    private List<Exception> ProcessModelResults(List<GenAiResultItem> items, DocumentsOperationContext context, List<Task<(string Result, string Usage)>> tasks, GenAiStatsScope statsScope)
    {
        List<Exception> exceptions = null;

        for (int index = 0; index < tasks.Count; index++)
        {
            var task = tasks[index];
            var item = items[index];
            if (task.IsCompletedSuccessfully is false)
            {
                var singleEx = task.Exception.ExtractSingleInnerException();

                if (Configuration.TestMode)
                    throw singleEx;

                switch (singleEx)
                {
                    case TooManyTokensException:
                    case RefusedToAnswerException:
                        var reason = singleEx is TooManyTokensException
                            ? "Token limit exceeded"
                            : "Model refused to answer";

                        var msg = $"Model call failed for context in document '{item.DocId}' ({reason}). " +
                                  $"Context was: {item.ContextOutput.Context}" + Environment.NewLine +
                                  singleEx.Message;
                        Statistics.RecordPartialLoadError(msg, item.DocId);
                        Logger.Log(LogLevel.Warn, msg);
                        break;
                    default:
                        item.UpdateHash = false;
                        break;
                }

                exceptions ??= [];
                exceptions.Add(singleEx);

                continue; // so we won't try to save it 
            }

            (string result, string usage) = task.Result;

            //TODO: REALLY YUCKY!
            var stream = new ReadOnlyMemoryStream<byte>(Encoding.UTF8.GetBytes(result));
            item.ModelOutput = new ModelOutput
            {
                Output = context.Sync.ReadForMemory(stream, item.DocId)
            };

            stream.Dispose();

            stream = new ReadOnlyMemoryStream<byte>(Encoding.UTF8.GetBytes(usage));
            var usageBlittable = context.Sync.ReadForMemory(stream, item.DocId);
            usageBlittable.TryGet("total_tokens", out int tokensUsed);
            usageBlittable.TryGet("prompt_tokens", out int promptTokens);
            usageBlittable.TryGet("completion_tokens", out int completionTokens);

            statsScope.TotalTokensUsed += tokensUsed;
            statsScope.PromptTokensUsed += promptTokens;
            statsScope.CompletionTokensUsed += completionTokens;

            if (Configuration.TestMode)
            {
                item.ModelOutput.Usage = usageBlittable;
            }

            stream.Dispose();
        }

        return exceptions;
    }

    private void ApplyUpdateScript(DocumentsOperationContext context, List<GenAiResultItem> results)
    {
        PatchRequest req = new(Configuration.UpdateScript, PatchRequestType.GenAi);
        var cmd = new GenAiBatchPatchCommand(context, results, req, Configuration.Name, Logger, Statistics);

        Database.TxMerger.EnqueueSync(cmd);
    }

    protected override GenAiStatsScope CreateScope(EtlRunStats stats)
    {
        return new GenAiStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "Gen AI";

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    public TestEtlScriptResult RunTest(TestGenAiScript testGenAiScript, DocumentsOperationContext context)
    {
        List<GenAiResultItem> items;
        List<Exception> exceptions = null;
        BlittableJsonReaderObject outputDocument = null;

        Document document;
        if (testGenAiScript.Document != null)
        {
            document = new Document
            {
                Data = testGenAiScript.Document, 
                ChangeVector = ChangeVectorUtils.NewChangeVector(context.DocumentDatabase.ServerStore.NodeTag, long.MaxValue, context.DocumentDatabase.DbBase64Id),
                Id = context.GetLazyString(TestDocumentId)
            };
        }
        else
        {
            if (string.IsNullOrEmpty(testGenAiScript.DocumentId))
                throw new InvalidOperationException("Document or DocumentId must be provided to run GenAI test");

            context.OpenReadTransaction();
            document = context.DocumentDatabase.DocumentsStorage.Get(context, testGenAiScript.DocumentId)?.Clone(context);
            if (document == null)
                throw new InvalidOperationException($"Document {testGenAiScript.DocumentId} does not exist");
        }

        using var scope = new GenAiStatsScope(new EtlRunStats());
        switch (testGenAiScript.TestStage)
        {
            case TestStage.CreateContextObjects:
                if (context.HasTransaction == false)
                    context.OpenReadTransaction();

                var genAiItem = new GenAiItem(document, Configuration.Collection);
                var transformedResults = Transform([genAiItem], context, scope, new EtlProcessState());
                items = PrepareItemsBeforeSendingToModel(transformedResults);

                context.CloseTransaction();
                break;
            case TestStage.SendToModel:
                _chatCompletionClient ??= GetClient();
                items = testGenAiScript.Input;
                exceptions = SendToModel(items, context, scope);
                if (exceptions is not null)
                    throw new AggregateException(exceptions);

                break;
            case TestStage.ApplyUpdateScript:
                {
                    context.CloseTransaction();
                    using var _ = context.OpenWriteTransaction();

                    items = testGenAiScript.Input;
                    PatchRequest req = new(Configuration.UpdateScript, PatchRequestType.GenAi);
                    PatchDocumentCommand lastPatch = null;
                    var hashes = new DynamicJsonArray();

                    if (testGenAiScript.Document != null)
                    {
                        // the document that was provided as input does not exist (we gave it a dummy id),
                        // so it needs to be written to storage before the patch.
                        // the write-tx is not commited so this won't be persisted.

                        if (document.Data.HasParent)
                        {
                            using (var old = document.Data)
                            {
                                document.Data = document.Data.Clone(context);
                            }
                        }

                        context.DocumentDatabase.DocumentsStorage.Put(context, document.Id, expectedChangeVector: null, document.Data);
                    }

                    foreach (var item in items)
                    {
                        hashes.Add(item.ContextOutput.AiHash);

                        if (item?.ModelOutput is null)
                            continue;

                        var dvj = new DynamicJsonValue
                        {
                            ["output"] = item.ModelOutput.Output,
                            ["input"] = item.ContextOutput.Context
                        };

                        var args = context.ReadObject(dvj, document.Id);
                        var cmd = lastPatch = new PatchDocumentCommand(
                            context: context,
                            id: document.Id,
                            expectedChangeVector: null,
                            skipPatchIfChangeVectorMismatch: false,
                            patch: (req, args),
                            patchIfMissing: default,
                            createIfMissing: null,
                            identityPartsSeparator: Database.IdentityPartsSeparator,
                            isTest: false,
                            debugMode: false,
                            collectResultsNeeded: true,
                            returnDocument: false,
                            ignoreMaxStepsForScript: false);

                        cmd.Execute(context, recordingState: null);

                        item.DebugActions = cmd.DebugActions;
                        item.DebugOutput = cmd.DebugOutput;
                    }

                    if (lastPatch?.PatchResult?.ModifiedDocument != null)
                        outputDocument = GenAiBatchPatchCommand.UpdateHashesInMetadata(document.Id, lastPatch.PatchResult.ModifiedDocument, Configuration.Name, hashes, context);

                    break;
                }

            default:
                throw new InvalidOperationException("Unknown TestStage type : " + testGenAiScript.TestStage.GetType());
        }

        return new GenAiTestScriptResult
        {
            InputDocument = document.Data,
            Results = items,
            OutputDocument = outputDocument,
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList()
        };
    }

    private static List<GenAiResultItem> PrepareItemsBeforeSendingToModel(IEnumerable<GenAiScriptResult> items)
    {
        // TODO we can do this in the transform phase 

        var results = new List<GenAiResultItem>();

        foreach (var scriptResult in items)
        {
            var item = new GenAiResultItem
            {
                DocId = scriptResult.DocumentId,

                ContextOutput = new ContextOutput
                {
                    Context = scriptResult.Context,
                    IsCached = scriptResult.IsCached,
                    AiHash = scriptResult.AiHash
                }
            };

            results.Add(item);
        }

        return results;
    }

    internal AbstractChatCompletionClient GetChatCompletionClient() => _chatCompletionClient;
}

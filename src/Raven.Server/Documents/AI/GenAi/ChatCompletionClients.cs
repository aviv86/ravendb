using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.AI.GenAi
{
    internal class OpenAiChatCompletionClient : AbstractChatCompletionClient
    {
        public OpenAiChatCompletionClient(GenAiConfiguration configuration, TransactionContextPool contextPool, DocumentConventions conventions) : base(baseUri: new Uri(configuration.Connection.OpenAiSettings.Endpoint),
            model: configuration.Connection.OpenAiSettings.Model, apiKey: configuration.Connection.OpenAiSettings.ApiKey,
            structuredOutputSchema: configuration.JsonSchema, contextPool, conventions)
        {
        }
    }

    internal class OllamaChatCompletionClient : AbstractChatCompletionClient
    {
        public OllamaChatCompletionClient(GenAiConfiguration configuration, TransactionContextPool contextPool, DocumentConventions conventions) : base(baseUri: new Uri(configuration.Connection.OllamaSettings.Uri),
            model: configuration.Connection.OllamaSettings.Model, apiKey: null, structuredOutputSchema: configuration.JsonSchema, contextPool, conventions)
        {
        }

    }
}

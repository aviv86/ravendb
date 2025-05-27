using System.Linq;
using System;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddGenAiCommand : AddEtlCommand<GenAiConfiguration, AiConnectionString>
{
    public AddGenAiCommand()
    {
        // for deserialization
    }

    public AddGenAiCommand(GenAiConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
    {

    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        Validate(record);

        Add(ref record.GenAiEtls, record, etag);
    }

    private void Validate(DatabaseRecord databaseRecord)
    {
        return;

        // TODO
        if (databaseRecord == null)
            throw new RachisApplyException("Failed to get database record, but it is required for further validation");

        if (string.IsNullOrWhiteSpace(Configuration.Identifier))
            throw new RachisApplyException("Integration task identifier must be set, but it is not");

        if (EmbeddingsGenerationConfiguration.ValidateIdentifier(Configuration.Identifier, out var errors) == false)
            throw new RachisApplyException($"Invalid identifier format. Validation errors:{Environment.NewLine} - {string.Join($"{Environment.NewLine} - ", errors)}");

        var isUpdate = databaseRecord.GenAiEtls.Any(x => x.Name == Configuration.Name);

        var identifierConflicts = databaseRecord?.GenAiEtls
            .Where(x => x.Identifier == Configuration.Identifier && x.Name != Configuration.Name)
            .ToArray();

        if (identifierConflicts.Length > 0)
            throw new RachisApplyException(
                $"Can't {(isUpdate ? "update" : "create")} GenAI task: '{Configuration.Name}'. " +
                $"The identifier '{Configuration.Identifier}' is already used by " +
                $"Gen AI task{(identifierConflicts.Length > 1 ? "s" : "")} " +
                $"'{string.Join("', '", identifierConflicts.Select(x => x.Name))}'");
    }
}

using Raven.Client.Documents.Smuggler;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreProgress : SmugglerResult.SmugglerProgress
    {
        public Counts SnapshotRestore => (_result as RestoreResult)?.SnapshotRestore;

        public FileCounts Files => (_result as RestoreResult)?.Files;

        public RestoreProgress()
            : this(null)
        {
            // for deserialization
        }

        public RestoreProgress(RestoreResult result) : base(result)
        {
            
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SnapshotRestore)] = SnapshotRestore?.ToJson();
            json[nameof(Files)] = Files?.ToJson();
            return json;
        }
    }
}

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiBackupRestore(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Ai | RavenTestCategory.Smuggler)]
    public async Task CanExportAndImportGenAiConfiguration()
    {
        var exportFile = GetTempFileName();

        using (var src = GetDocumentStore())
        using (var dst = GetDocumentStore())
        {
            var config = new GenAiConfiguration
            {
                Name = "SmugglerTestTask",
                ConnectionStringName = "ollama-local",
                Prompt = "Translate the following sentence",
                Collection = "Posts",
                SampleObject = JsonConvert.SerializeObject(new { Translation = "foo" }),
                UpdateScript = "this.Translation = $output.Translation",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = "context({ Sentence: this.Body });"
                }
            };

            src.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
            {
                Name = "ollama-local",
                Identifier = "ollama-local",
                OllamaSettings = new OllamaSettings
                {
                    Uri = "http://127.0.0.1:11434/",
                    Model = "llama3.2:latest"
                }
            }));

            src.Maintenance.Send(new AddGenAiOperation(config));

            await (await src.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile)).WaitForCompletionAsync();
            await (await dst.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile)).WaitForCompletionAsync();

            var dstRecord = await dst.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dst.Database));
            Assert.Equal(1, dstRecord.GenAiEtls.Count);
            Assert.Equal(1, dstRecord.AiConnectionStrings.Count);

            var imported = dstRecord.GenAiEtls.First();
            Assert.Equal(config.Name, imported.Name);
            Assert.Equal(config.ConnectionStringName, imported.ConnectionStringName);
            Assert.Equal(config.Prompt, imported.Prompt);
            Assert.Equal(config.SampleObject, imported.SampleObject);
            Assert.Equal(config.UpdateScript, imported.UpdateScript);
            Assert.Equal(config.Collection, imported.Collection);
            Assert.Equal(config.GenAiTransformation.Script, imported.GenAiTransformation.Script);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
    [InlineData(BackupType.Backup)]
    [InlineData(BackupType.Snapshot)]
    public async Task CanBackupAndRestoreGenAiEtl(BackupType backupType)
    {
        var backupPath = NewDataPath();
        var sampleObject = JsonConvert.SerializeObject(new { Answer = "42" });

        using (var store = GetDocumentStore())
        {
            var config = new GenAiConfiguration
            {
                Name = "TestGenAiTask",
                ConnectionStringName = "ollama-local",
                Prompt = "Give a short answer to the following question",
                Collection = "Posts",
                SampleObject = sampleObject,
                UpdateScript = "this.GenAnswer = $output.Answer",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = "context({ Question: this.Body });"
                }
            };

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
            {
                Name = "ollama-local",
                Identifier = "ollama-local",
                OllamaSettings = new OllamaSettings
                {
                    Uri = "http://127.0.0.1:11434/",
                    Model = "llama3.2:latest"
                }
            }));

            store.Maintenance.Send(new AddGenAiOperation(config));

            var etlDone = Etl.WaitForEtlToComplete(store);

            // Add a doc
            const string id = "posts/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new GenAiBasics.Post([new GenAiBasics.Comment("42", "Douglas Adams")], "What is the answer to life?", "So long, and Thanks"), id);
                await session.SaveChangesAsync();
            }

            Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

            string srcHash;
            using (var session = store.OpenSession())
            {
                var doc = session.Load<BlittableJsonReaderObject>(id);
                Assert.True(doc.TryGet("GenAnswer", out string genAnswer));
                Assert.NotNull(genAnswer);

                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
                Assert.Equal(1, hashesSection.Count);

                Assert.True(hashesSection.TryGet(config.Name, out BlittableJsonReaderArray hashes));
                Assert.Equal(1, hashes.Length);

                srcHash = hashes.Single().ToString();
            }

            var srcDb = await GetDatabase(store.Database);

            var srcState = EtlProcess.GetProcessState(srcDb, config.Name, config.Transforms[0].Name);
            var srcLastProcessedEtag = srcState.GetLastProcessedEtag(srcDb.DbBase64Id, Server.ServerStore.NodeTag);
            Assert.True(srcLastProcessedEtag > 0);

            // Perform backup
            var backupOp = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = backupType,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            var result = (BackupResult)await backupOp.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            var backupDir = Path.Combine(backupPath, result.LocalBackup.BackupDirectory);

            var restoredDb = $"{store}_Restore";

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupDir,
                DatabaseName = restoredDb
            }))
            {
                var src = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var dst = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDb));

                Assert.Equal(src.AiConnectionStrings.Count, dst.AiConnectionStrings.Count);
                Assert.Equal(src.GenAiEtls.Count, dst.GenAiEtls.Count);

                var srcGenConfig = src.GenAiEtls.First();
                var dstGenConfig = dst.GenAiEtls.First();

                Assert.Equal(srcGenConfig.Name, dstGenConfig.Name);
                Assert.Equal(srcGenConfig.ConnectionStringName, dstGenConfig.ConnectionStringName);
                Assert.Equal(srcGenConfig.Prompt, dstGenConfig.Prompt);
                Assert.Equal(srcGenConfig.JsonSchema, dstGenConfig.JsonSchema);
                Assert.Equal(srcGenConfig.UpdateScript, dstGenConfig.UpdateScript);
                Assert.Equal(srcGenConfig.Collection, dstGenConfig.Collection);
                Assert.Equal(srcGenConfig.GenAiTransformation.Script, dstGenConfig.GenAiTransformation.Script);

                var dstDb = await GetDatabase(restoredDb);

                var value = await WaitForValueAsync(() =>
                {
                    var dstState = EtlProcess.GetProcessState(dstDb, config.Name, config.Transforms[0].Name);
                    var lastProcessedEtag = dstState.GetLastProcessedEtag(dstDb.DbBase64Id, Server.ServerStore.NodeTag);
                    return Task.FromResult(lastProcessedEtag > 0);
                }, true, timeout: 60_000);

                Assert.True(value);

                using (var session = store.OpenSession(restoredDb))
                {
                    var doc = session.Load<BlittableJsonReaderObject>(id);
                    Assert.NotNull(doc);

                    Assert.True(doc.TryGet("GenAnswer", out string genAnswer));
                    Assert.NotNull(genAnswer);

                    Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
                    Assert.Equal(1, hashesSection.Count);

                    Assert.True(hashesSection.TryGet(config.Name, out BlittableJsonReaderArray hashes));
                    Assert.Equal(1, hashes.Length);

                    var dstHash = hashes.Single().ToString();

                    Assert.Equal(srcHash, dstHash);
                }

            }
        }
    }

    [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Etl | RavenTestCategory.Ai | RavenTestCategory.BackupExportImport)]
    public async Task CanBackupAndRestoreGenAiInShardedDatabase()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using (var store = Sharding.GetDocumentStore())
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
            {
                Name = "ollama-local",
                Identifier = "ollama-local",
                OllamaSettings = new OllamaSettings
                {
                    Uri = "http://127.0.0.1:11434/",
                    Model = "llama3.2:latest"
                }
            }));

            var config = new GenAiConfiguration
            {
                Name = "TestGenAiTask",
                ConnectionStringName = "ollama-local",
                Prompt = "What is the answer to life?",
                Collection = "Posts",
                SampleObject = JsonConvert.SerializeObject(new { Answer = "42" }),
                UpdateScript = "this.GenAnswer = $output.Answer",
                GenAiTransformation = new GenAiTransformation
                {
                    Script = "context({ Question: this.Body });"
                }
            };

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));

            var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

            var backupDirs = Directory.GetDirectories(backupPath);
            var sharding = await Sharding.GetShardingConfigurationAsync(store);
            var settings = Sharding.Backup.GenerateShardRestoreSettings(backupDirs, sharding);

            var restoreName = $"{store}_Restored";

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                DatabaseName = restoreName,
                ShardRestoreSettings = settings
            }))
            {
                var restored = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoreName));
                Assert.Equal(1, restored.GenAiEtls.Count);
                Assert.Equal(1, restored.AiConnectionStrings.Count);

                var restoredGenConfig = restored.GenAiEtls.First();

                Assert.Equal(config.Name, restoredGenConfig.Name);
                Assert.Equal(config.ConnectionStringName, restoredGenConfig.ConnectionStringName);
                Assert.Equal(config.Prompt, restoredGenConfig.Prompt);
                Assert.Equal(config.SampleObject, restoredGenConfig.SampleObject);
                Assert.Equal(config.UpdateScript, restoredGenConfig.UpdateScript);
                Assert.Equal(config.Collection, restoredGenConfig.Collection);
                Assert.Equal(config.GenAiTransformation.Script, restoredGenConfig.GenAiTransformation.Script);
            }
        }
    }

}

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Authentication;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Utils;
using Size = Sparrow.Size;
using SizeClient = Raven.Client.Util.Size;

namespace Raven.Server.Utils.Cli
{
    public class RavenCli
    {
        public const string DelimiterKeyWord = "DELIMITER:";

        public enum Delimiter
        {
            NotFound,
            ReadLine,
            ReadKey,
            Clear,
            Logout,
            Shutdown,
            RestartServer,
            ContinuePrinting
        }

        public static string GetDelimiterString(Delimiter delimiter) => DelimiterKeyWord + delimiter;

        private static readonly Action<List<string>, bool, RavenServer, TextWriter> Prompt = (list, test, server, writer) =>
        {
            var msg = new StringBuilder();
            var first = true;
            foreach (var l in list)
            {
                if (first == false)
                    msg.Append(" ");
                else
                    first = false;

                switch (l)
                {
                    case "%D":
                        msg.Append(DateTime.UtcNow.ToString("yyyy/MMM/dd"));
                        break;
                    case "%T":
                        msg.Append(DateTime.UtcNow.ToString("HH:mm:ss"));
                        break;
                    case "%M":
                        {
                            var memoryStats = MemoryStatsWithMemoryMappedInfo();
                            msg.Append($"WS:{memoryStats.WorkingSet}");
                            msg.Append($"|UM:{memoryStats.TotalUnmanagedAllocations}");
                            msg.Append($"|M:{memoryStats.ManagedMemory}");
                            msg.Append($"|MP:{memoryStats.TotalMemoryMapped}");
                        }
                        break;
                    case "%R":
                        {
                            var reqCounter = server.Metrics.Requests.RequestsPerSec;
                            msg.Append($"Req/Sec:{Math.Round(reqCounter.OneSecondRate, 1)}");
                        }
                        break;

                    default:
                        msg.Append(l);
                        break;
                }
            }
            if (test == false)
                writer.Write(msg);
        };

        private TextWriter _writer;
        private TextReader _reader;
        private RavenServer _server;
        private bool _experimental;
        private bool _consoleColoring;

        private enum Command
        {
            // ReSharper disable once UnusedMember.Local
            None,
            Prompt,
            HelpPrompt,
            Shutdown,
            Log,
            Timer,
            Clear,
            ResetServer,
            Stats,
            Info,
            Gc,
            TrustServerCert,
            TrustClientCert,
            GenerateClientCert,
            ReplaceClusterCert,
            TriggerCertificateRefresh,
            LowMem,
            Help,
            Logo,
            Experimental,
            Script,
            ImportDir,
            CreateDb,
            Logout,
            Print,

            UnknownCommand
        }

        private enum LineState
        {
            // ReSharper disable once UnusedMember.Local
            None,
            Begin,
            AfterCommand,
            AfterArgs,
            Empty
        }

        private enum ConcatAction
        {
            // ReSharper disable once UnusedMember.Local
            None,
            And,
            Or
        }

        private class ParsedCommand
        {
            public Command Command;
            public ConcatAction PrevConcatAction;
            public List<string> Args;
        }

        private class ParsedLine
        {
            public LineState LineState;
            public string ErrorMsg;
            public readonly List<ParsedCommand> ParsedCommands = new List<ParsedCommand>();
        }

        private List<string> _promptArgs = new List<string> { "ravendb" };

        private const ConsoleColor PromptHeaderColor = ConsoleColor.Magenta;
        private const ConsoleColor PromptArrowColor = ConsoleColor.Cyan;
        private const ConsoleColor UserInputColor = ConsoleColor.Green;
        private const ConsoleColor WarningColor = ConsoleColor.Yellow;
        private const ConsoleColor TextColor = ConsoleColor.White;
        private const ConsoleColor ErrorColor = ConsoleColor.Red;

        private class SingleAction
        {
            public int NumOfArgs;
            public Func<List<string>, RavenCli, bool> DelegateFync;
            public bool Experimental { get; set; }
        }

        private static void PrintCliHeader(RavenCli cli)
        {
            if (cli._consoleColoring)
                Console.ForegroundColor = PromptHeaderColor;
            try
            {
                Prompt.Invoke(cli._promptArgs, false, cli._server, cli._writer);
            }
            catch (Exception ex)
            {
                WriteError("PromptError:" + ex.Message, cli);
            }
            WriteText("> ", PromptArrowColor, cli, newLine: false);
            if (cli._consoleColoring)
                Console.ForegroundColor = UserInputColor;
        }

        private static void WriteError(string txt, RavenCli cli)
        {
            WriteText($"ERROR: {txt}", ErrorColor, cli);
            WriteText("", TextColor, cli);
        }

        private static void WriteWarning(string txt, RavenCli cli)
        {
            WriteText($"WARNING: {txt}", WarningColor, cli);
            WriteText("", TextColor, cli);
        }

        private static void WriteText(string txt, ConsoleColor color, RavenCli cli, bool newLine = true)
        {
            if (cli._consoleColoring)
                Console.ForegroundColor = color;
            cli._writer.Write(txt);
            if (newLine)
                cli._writer.WriteLine();
            if (cli._consoleColoring)
                Console.ResetColor();
            cli._writer.Flush();
        }

        private static char ReadKey(RavenCli cli)
        {
            if (cli._consoleColoring)
            {
                var rc = Console.ReadKey().KeyChar;
                cli._writer.Flush();
                return rc;
            }

            cli._writer.Write(GetDelimiterString(Delimiter.ReadKey));
            cli._writer.Flush();
            var c = cli._reader.Read();
            return Convert.ToChar(c);
        }

        private string ReadLine(RavenCli cli)
        {
            if (cli._consoleColoring == false)
            {
                cli._writer.Write(GetDelimiterString(Delimiter.ReadLine));
                cli._writer.Flush();
            }

            var rc = _consoleColoring ? Console.ReadLine() : cli._reader.ReadLine();
            cli._writer.Flush();
            return rc;
        }

        private static bool CommandLogout(List<string> args, RavenCli cli)
        {
            if (cli._consoleColoring)
            {
                WriteText("'logout' command not supported on console cli", WarningColor, cli);
                return true;
            }

            cli._writer.WriteLine("Logging out..", TextColor, cli);
            cli._writer.Write(GetDelimiterString(Delimiter.Logout));
            cli._writer.Flush();
            return true;
        }

        private static bool CommandShutdown(List<string> args, RavenCli cli)
        {
            WriteText("", TextColor, cli);
            WriteText("Are you sure you want to shutdown the server ? [y/N] : ", TextColor, cli, newLine: false);

            var k = ReadKey(cli);
            WriteText("", TextColor, cli);

            return char.ToLower(k).Equals('y');
        }

        private static bool CommandResetServer(List<string> args, RavenCli cli)
        {
            WriteText("", TextColor, cli);
            WriteText("Are you sure you want to reset the server ? [y/N] : ", TextColor, cli, newLine: false);

            var k = ReadKey(cli);
            WriteText("", TextColor, cli);

            return char.ToLower(k).Equals('y');
        }

        private static bool CommandStats(List<string> args, RavenCli cli)
        {
            if (cli._consoleColoring == false)
            {
                // beware not to allow this from remote - will disable local console!                
                WriteText("'stats' command not supported on remote pipe connection. Use `info` or `prompt %M` instead", TextColor, cli);
                return true;
            }

            Console.ResetColor();

            LoggingSource.Instance.DisableConsoleLogging();
            var prevLogMode = LoggingSource.Instance.LogMode;
            LoggingSource.Instance.SetupLogMode(LogMode.None, cli._server.Configuration.Logs.Path.FullPath);
            Program.WriteServerStatsAndWaitForEsc(cli._server);
            LoggingSource.Instance.SetupLogMode(prevLogMode, cli._server.Configuration.Logs.Path.FullPath);
            Console.WriteLine($"LogMode set back to {prevLogMode}.");
            return true;
        }

        private static bool CommandPrompt(List<string> args, RavenCli cli)
        {
            try
            {
                Prompt.Invoke(args, true, cli._server, cli._writer);
            }
            catch (Exception ex)
            {
                WriteError("Cannot set prompt to desired args, because of : " + ex.Message, cli);
                return false;
            }
            return true;
        }

        private static bool CommandHelpPrompt(List<string> args, RavenCli cli)
        {
            string[][] commandDescription =
            {
                new[] {"%D", "UTC Date"},
                new[] {"%T", "UTC Time"},
                new[] {"%M", "Memory information (WS:WorkingSet, UM:Unmanaged, M:Managed, MP:MemoryMapped)"},
                new[] {"%R", "Momentary Req/Sec"},
                new[] {"label", "any label"}
            };

            var msg = new StringBuilder();
            msg.Append("Usage: prompt <[label] | [ %D | %T | %M ] | ...>" + Environment.NewLine + Environment.NewLine);
            msg.Append("Options:" + Environment.NewLine);
            WriteText(msg.ToString(), TextColor, cli);

            foreach (var cmd in commandDescription)
            {
                WriteText("\t" + cmd[0], ConsoleColor.Yellow, cli, newLine: false);
                WriteText(new string(' ', 25 - cmd[0].Length) + cmd[1], ConsoleColor.DarkYellow, cli);
            }
            return true;
        }

        private static bool CommandGc(List<string> args, RavenCli cli)
        {
            var genNum = args == null || args.Count == 0 ? 2 : Convert.ToInt32(args.First());

            WriteText("Before collecting, managed memory used: ", TextColor, cli, newLine: false);
            WriteText(new Size(GC.GetTotalMemory(false), SizeUnit.Bytes).ToString(), ConsoleColor.Cyan, cli);
            var startTime = DateTime.UtcNow;
            WriteText("Garbage Collecting... ", TextColor, cli, newLine: false);

            switch (genNum)
            {
                case 0:
                    GC.Collect(0);
                    break;
                case 1:
                    GC.Collect(1);
                    break;
                case 2:
                    GC.Collect(GC.MaxGeneration);
                    break;
                default:
                    WriteError("Invalid argument passed to GC. Can be 0, 1 or 2", cli);
                    return false;
            }

            GC.WaitForPendingFinalizers();
            var actionTime = DateTime.UtcNow - startTime;

            WriteText("Collected.", ConsoleColor.Green, cli);
            WriteText("After collecting, managed memory used:  ", TextColor, cli, newLine: false);
            WriteText(new Size(GC.GetTotalMemory(false), SizeUnit.Bytes).ToString(), ConsoleColor.Cyan, cli, newLine: false);
            WriteText(" at ", TextColor, cli, newLine: false);
            WriteText(actionTime.TotalSeconds + " Seconds", ConsoleColor.Cyan, cli);
            return true;
        }

        private static bool CommandTimer(List<string> args, RavenCli cli)
        {
            switch (args.First())
            {
                case "on":
                    cli._server.ServerStore.Engine.Timeout.Disable = false;
                    WriteText("Timer enabled", TextColor, cli);
                    break;
                case "off":
                    cli._server.ServerStore.Engine.Timeout.Disable = true;
                    WriteText("Timer disabled", TextColor, cli);
                    break;
                case "fire":
                    cli._server.ServerStore.Engine.Timeout.ExecuteTimeoutBehavior();
                    WriteText("Timer fired", TextColor, cli);
                    break;
            }

            return true;
        }

        private static bool CommandLog(List<string> args, RavenCli cli)
        {
            var withConsole = !(args.Count == 2 && args[1].Equals("no-console"));
                        
            switch (args.First())
            {
                case "on":
                case "information":
                    if (withConsole)
                        LoggingSource.Instance.EnableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.Information, cli._server.Configuration.Logs.Path.FullPath);
                    WriteText("Logging set to ON (information)", ConsoleColor.Green, cli);
                    break;
                case "off":
                case "none":
                    LoggingSource.Instance.DisableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.None, cli._server.Configuration.Logs.Path.FullPath);
                    WriteText("Logging set to OFF (none)", ConsoleColor.DarkGreen, cli);
                    break;
                case "operations":
                    if (withConsole)
                        LoggingSource.Instance.EnableConsoleLogging();
                    LoggingSource.Instance.SetupLogMode(LogMode.None, cli._server.Configuration.Logs.Path.FullPath);
                    WriteText("Logging set to ON (operations)", ConsoleColor.DarkGreen, cli);
                    break;
                case "http-off":
                    WriteText("Setting HTTP logging OFF", ConsoleColor.DarkGreen, cli);
                    RavenServerStartup.SkipHttpLogging = true;
                    goto case "on";
                case "http-on":
                    WriteText("Setting HTTP logging ON", ConsoleColor.Green, cli);
                    RavenServerStartup.SkipHttpLogging = false;
                    goto case "on";
            }

            return true;
        }

        private static bool CommandClear(List<string> args, RavenCli cli)
        {
            if (cli._consoleColoring)
                Console.Clear();
            else
                cli._writer.Write(GetDelimiterString(Delimiter.Clear));
            cli._writer.Flush();
            return true;
        }

        private static bool CommandInfo(List<string> args, RavenCli cli)
        {
            new ClusterMessage(Console.Out, cli._server.ServerStore).Print();

            WriteText(GetInfoText(), ConsoleColor.Cyan, cli);

            if (cli._server.Configuration.Storage.ForceUsing32BitsPager || IntPtr.Size == sizeof(int))
                WriteText(" Running in 32 bits mode", ConsoleColor.DarkCyan, cli);

            return true;
        }

        public static string GetInfoText()
        {
            var memoryInfo = MemoryInformation.GetMemoryInfo();
            using (var currentProcess = Process.GetCurrentProcess())
            {
                return $" Build {ServerVersion.Build}, Version {ServerVersion.Version}, SemVer {ServerVersion.FullVersion}, Commit {ServerVersion.CommitHash}" +
                       Environment.NewLine +
                       $" PID {currentProcess.Id}, {IntPtr.Size * 8} bits, {ProcessorInfo.ProcessorCount} Cores, Arch: {RuntimeInformation.OSArchitecture}" +
                       Environment.NewLine +
                       $" {memoryInfo.TotalPhysicalMemory} Physical Memory, {memoryInfo.AvailableMemory} Available Memory" +
                       Environment.NewLine +
                       $" {RuntimeSettings.Describe()}";
            }
        }

        private static bool CommandLogo(List<string> args, RavenCli cli)
        {
            if (args == null || args.Count == 0 || args.First().Equals("no-clear") == false)
                if (cli._consoleColoring)
                    Console.Clear();
            if (cli._consoleColoring)
                new WelcomeMessage(Console.Out).Print();
            else
                new WelcomeMessage(cli._writer).Print();
            return true;
        }

        private static bool CommandExperimental(List<string> args, RavenCli cli)
        {
            var isOn = args.First().Equals("on");
            var isOff = args.First().Equals("off");
            if (!isOff && !isOn)
            {
                WriteError("Experimental cli commands can be set to only on or off. Setting to off.", cli);
                return false;
            }

            return isOn; // here rc is not an exit code, it is a setter to _experimental
        }

        private const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";
        private const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";

        private static bool CommandTrustServerCert(List<string> args, RavenCli cli)
        {
            if (args.Count < 2 || args.Count > 3)
            {
                WriteError("Usage: trustServerCert <name> <path-to-pfx> [password]", cli);
                return false;
            }

            var name = args[0];
            var path = args[1];
            
            X509Certificate2 cert;
            try
            {
                cert = args.Count == 3 ? new X509Certificate2(path, args[2]) : new X509Certificate2(path);
            }
            catch (Exception e)
            {
                WriteError("Failed to load the provided certificate. Please check the path and password." + e, cli);
                return false;
            }

            var serverAuth = false;
            var clientAuth = false;
            foreach (var extension in cert.Extensions.OfType<X509EnhancedKeyUsageExtension>())
            {
                foreach (var oid in extension.EnhancedKeyUsages)
                {
                    if (oid.Value.Equals(ServerAuthenticationOid, StringComparison.Ordinal))
                        serverAuth = true;
                    if (oid.Value.Equals(ClientAuthenticationOid, StringComparison.Ordinal))
                        clientAuth = true;
                }
            }

            if (clientAuth == false || serverAuth == false)
            {
                WriteError($"Certificate {cert.Thumbprint} cannot be a ravendb server certificate. It does not include both Extended Key Usages: Server Authentication ({ServerAuthenticationOid}), Client Authentication ({ClientAuthenticationOid}).", cli);
                return false;
            }

            WriteText("Successfully read certificate: " + cert.Thumbprint, TextColor, cli);
            WriteText(cert.ToString(), TextColor, cli);

            var certKey = Constants.Certificates.Prefix + cert.Thumbprint;

            using (cli._server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var certDef = new CertificateDefinition
                {
                    Name = name,
                    // this does not include the private key, that is only for the client
                    Certificate = Convert.ToBase64String(cert.Export(X509ContentType.Cert)),
                    Permissions = new Dictionary<string, DatabaseAccess>(),
                    SecurityClearance = SecurityClearance.ClusterNode,
                    Thumbprint = cert.Thumbprint,
                    NotAfter = cert.NotAfter
                };
                
                try
                {
                    if (cli._server.ServerStore.CurrentRachisState == RachisState.Passive)
                    {
                        using (var certificate = ctx.ReadObject(certDef.ToJson(), "Server/Certificate/Definition"))
                        using (var tx = ctx.OpenWriteTransaction())
                        {
                            cli._server.ServerStore.Cluster.PutLocalState(ctx, certKey, certificate);
                            tx.Commit();
                        }
                    }
                    else
                    {
                        var putResult = cli._server.ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + certDef.Thumbprint, certDef)).Result;
                        cli._server.ServerStore.Cluster.WaitForIndexNotification(putResult.Index).Wait();
                    }
                }
                catch (Exception e)
                {
                    WriteError($"Failed to store cerrificate {cert.Thumbprint} in the server." + e, cli);
                    return false;
                }

                WriteText("Successfully registered the server certificate" + cert.Thumbprint, TextColor, cli);
            }

            return true;
        }

        private static bool CommandTrustClientCert(List<string> args, RavenCli cli)
        {
            if (args.Count < 2 || args.Count > 3)
            {
                WriteError("Usage: trustClientCert <name> <path-to-pfx> [password]", cli);
                return false;
            }

            var name = args[0];
            var path = args[1];
            var password = (args.Count == 3 ? args[2] : null);

            byte[] certBytes;
            X509Certificate2 cert;
            try
            {
                certBytes = File.ReadAllBytes(path);
                cert = password != null ? new X509Certificate2(certBytes, password) : new X509Certificate2(certBytes);
            }
            catch (Exception e)
            {
                WriteError("Failed to load the provided certificate. Please check the path and password." + e, cli);
                return false;
            }

            var clientAuth = false;
            foreach (var extension in cert.Extensions.OfType<X509EnhancedKeyUsageExtension>())
            {
                foreach (var oid in extension.EnhancedKeyUsages)
                {
                    if (oid.Value.Equals(ClientAuthenticationOid, StringComparison.Ordinal))
                        clientAuth = true;
                }
            }

            if (clientAuth == false)
            {
                WriteError($"Certificate {cert.Thumbprint} cannot be used as a client certificate. It does not include the Extended Key Usage: Client Authentication ({ClientAuthenticationOid}).", cli);
                return false;
            }

            WriteText("Successfully read certificate: " + cert.Thumbprint, TextColor, cli);
            WriteText(cert.ToString(), TextColor, cli);

            using (cli._server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var certDef = new CertificateDefinition
                {
                    Name = name,
                    // this does not include the private key, that is only for the client
                    Certificate = Convert.ToBase64String(cert.Export(X509ContentType.Cert)),
                    Permissions = new Dictionary<string, DatabaseAccess>(),
                    SecurityClearance = SecurityClearance.ClusterAdmin,
                    Thumbprint = cert.Thumbprint,
                    NotAfter = cert.NotAfter
                };
                
                try
                {
                    AdminCertificatesHandler.PutCertificateCollectionInCluster(certDef, certBytes, password,  cli._server.ServerStore, ctx).Wait();
                }
                catch (Exception e)
                {
                    WriteError($"Failed to put cerrificate {cert.Thumbprint} in the server." + e, cli);
                    return false;
                }

                WriteText("Successfully registered the client certificate " + cert.Thumbprint, TextColor, cli);
            }

            return true;
        }

        private static bool CommandGenerateClientCert(List<string> args, RavenCli cli)
        {
            if (args.Count < 2 || args.Count > 3)
            {
                WriteError("Usage: generateClientCert <name> <path-to-output-folder> [password]", cli);
                return false;
            }

            var name = args[0];
            var path = args[1];

            cli._server.ServerStore.EnsureNotPassive();

            var certDef = new CertificateDefinition
            {
                Name = name,
                Permissions = new Dictionary<string, DatabaseAccess>(),
                SecurityClearance = SecurityClearance.ClusterAdmin,
                Password = args.Count == 3 ? args[2] : null
            };

            byte[] outputBytes;
            try
            {
                outputBytes = AdminCertificatesHandler.GenerateCertificateInternal(certDef, cli._server.ServerStore).Result;
            }
            catch (Exception e)
            {
                WriteError("Failed to generate a client certificate." + e, cli);
                return false;
            }

            var certPath = "";
            try
            {
                var certificateFileName = $"admin.client.certificate.{name}.zip";
                certPath = Path.Combine(path, certificateFileName);

                using (var certfile = SafeFileStream.Create(certPath, FileMode.Create))
                {
                    certfile.Write(outputBytes, 0, outputBytes.Length);
                    certfile.Flush(true);
                }
            }
            catch (Exception e)
            {
                WriteError("Failed save the generated certificate to path: "+ certPath + e, cli);
                return false;
            }

            WriteText("Successfully saved the client certificate to " + certPath, TextColor, cli);

            return true;
        }
        
        private static bool CommandReplaceClusterCert(List<string> args, RavenCli cli)
        {
            if (args.Count < 2 || args.Count > 4)
            {
                WriteError("Usage: replaceClusterCert [-replaceImmediately] <name> <path-to-pfx> [password]", cli);
                return false;
            }

            string name;
            string path;
            string password = null;
            var replaceImmediately = false;

            if (args[0].Equals("-replaceImmediately"))
            {
                replaceImmediately = true;
                name = args[1];
                path = args[2];
                if (args.Count == 4)
                    password = args[3];
            }
            else
            {
                name = args[0];
                path = args[1];
                if (args.Count == 3)
                    password = args[2];
            }

            cli._server.ServerStore.EnsureNotPassive();

            // This restriction should be removed when updating to .net core 2.1 when export of collection is fixed.
            // With export, we'll be able to load the certificate and export it without a password, and propogate it through the cluster.
            if (string.IsNullOrWhiteSpace(password) == false)
                throw new NotSupportedException("Replacing the cluster certificate with a password protected certificates is currently not supported.");

            X509Certificate2 cert;
            byte[] certBytes;
            try
            {
                certBytes = File.ReadAllBytes(path);
                cert = new X509Certificate2(path, password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet);
            }
            catch (Exception e)
            {
                WriteError("Failed to load the provided certificate. Please check the path and password." + e, cli);
                return false;
            }

            WriteText("Successfully read certificate: " + cert.Thumbprint, TextColor, cli);
            WriteText(cert.ToString(), TextColor, cli);

            try
            {
                var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromSeconds(60), cli._server.ServerStore.ServerShutdown);

                var replicationTask = cli._server.ServerStore.Server.StartCertificateReplicationAsync(Convert.ToBase64String(certBytes), name, replaceImmediately);

                Task.WhenAny(replicationTask, timeoutTask).Wait();
                if (replicationTask.IsCompleted == false)
                    throw new TimeoutException("Timeout when trying to replace the server certificate.");
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to replace the server certificate. Check the logs for details.", e);
            }

            WriteText("Successfully replaced the server certificate.", TextColor, cli);

            return true;
        }

        private static bool CommandTriggerCertificateRefresh(List<string> args, RavenCli cli)
        {
            if (args.Count < 0 || args.Count > 1)
            {
                WriteError("Usage: triggerCertificateRefresh [-replaceImmediately]", cli);
                return false;
            }

            var replaceImmediately = args[0] != null && args[0].Equals("-replaceImmediately");
            
            try
            {
                cli._server.RefreshClusterCertificate(replaceImmediately);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to trigger a certificate refresh cycle.", e);
            }

            WriteText("Triggered a certificate refresh cycle.", TextColor, cli);

            return true;
        }

        private static bool CommandScript(List<string> args, RavenCli cli)
        {
            // script <database|server> [databaseName]
            if (args.Count < 1 || args.Count > 2)
            {
                WriteError("Invalid number of arguments passed to script", cli);
                return false;
            }

            DocumentDatabase database = null;
            switch (args[0].ToLower())
            {
                case "database":
                    if (args.Count != 2)
                    {
                        WriteError("Invalid number of arguments passed to script - missing database name", cli);
                        return false;
                    }
                    database = cli._server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(args[1]).Result;
                    if (database == null)
                    {
                        WriteError($"Cannot find database named '{args[1]}'", cli);
                        return false;
                    }
                    break;
                case "server":
                    break;
                default:
                    WriteError($"Invalid arguments '{args[0]}' passed to script", cli);
                    return false;
            }

            var jsCli = new JavaScriptCli();
            if (jsCli.CreateScript(cli._reader, cli._writer, cli._consoleColoring, database, cli._server) == false)
            {
                WriteError("Invalid JavaScript entered, or user cancelled", cli);
                return false;
            }

            using (cli._server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var adminJsScript = new AdminJsScript(jsCli.Script);
                var result = jsCli.AdminConsole.ApplyScript(adminJsScript);

                if (cli._consoleColoring)
                    Console.ForegroundColor = ConsoleColor.Magenta;

                WriteText(result, TextColor, cli);

                if (cli._consoleColoring)
                    Console.ResetColor();
            }
            return true;
        }

        public static string ConvertResultToString(ScriptRunnerResult result)
        {
            var ms = new MemoryStream();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(ctx, ms))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Result");

                if (result.IsNull)
                {
                    writer.WriteNull();
                }
                else if (result.RawJsValue.IsBoolean())
                {
                    writer.WriteBool(result.RawJsValue.AsBoolean());
                }
                else if (result.RawJsValue.IsString())
                {
                    writer.WriteString(result.RawJsValue.AsString());
                }
                else if (result.RawJsValue.IsDate())
                {
                    var date = result.RawJsValue.AsDate();
                    writer.WriteString(date.ToDateTime().ToString(DefaultFormat.DateTimeOffsetFormatsToWrite));
                }
                else if (result.RawJsValue.IsNumber())
                {
                    writer.WriteDouble(result.RawJsValue.AsNumber());
                }
                else
                {
                    writer.WriteObject(result.TranslateToObject(ctx));
                }

                writer.WriteEndObject();
                writer.Flush();
            }

            var str = Encoding.UTF8.GetString(ms.ToArray());
            return str;
        }

        private static bool CommandLowMem(List<string> args, RavenCli cli)
        {
            WriteText("Before simulating low-mem, memory stats: ", TextColor, cli, newLine: false);

            var memoryStats = MemoryStatsWithMemoryMappedInfo();
            var msg = new StringBuilder();
            msg.Append($"Working Set: {memoryStats.WorkingSet}");
            msg.Append($" Unmamanged Memory: {memoryStats.TotalUnmanagedAllocations}");
            msg.Append($" Managed Memory: {memoryStats.ManagedMemory}");
            WriteText(msg.ToString(), ConsoleColor.Cyan, cli);

            WriteText("Sending Low Memory simulation signal... ", TextColor, cli, newLine: false);
            LowMemoryNotification.Instance.SimulateLowMemoryNotification();
            WriteText("Sent.", ConsoleColor.Green, cli);

            WriteText("After sending low mem simulation event, memory stats: ", TextColor, cli, newLine: false);
            msg.Clear();
            msg.Append($"Working Set: {memoryStats.WorkingSet}");
            msg.Append($" Unmamanged Memory: {memoryStats.TotalUnmanagedAllocations}");
            msg.Append($" Managed Memory: {memoryStats.ManagedMemory}");
            WriteText(msg.ToString(), ConsoleColor.Cyan, cli);

            return true;
        }

        public static (
            string WorkingSet,
            string TotalUnmanagedAllocations,
            string ManagedMemory,
            string TotalMemoryMapped) MemoryStatsWithMemoryMappedInfo()
        {
            var stats = MemoryInformation.MemoryStats();

            long totalMemoryMapped = 0;
            foreach (var mapping in NativeMemory.FileMapping)
            {
                var maxMapped = 0L;
                foreach (var singleMapping in mapping.Value)
                {
                    maxMapped = Math.Max(maxMapped, singleMapping.Value);
                }

                totalMemoryMapped += maxMapped;
            }

            return (
                SizeClient.Humane(stats.WorkingSet),
                SizeClient.Humane(stats.TotalUnmanagedAllocations),
                SizeClient.Humane(stats.ManagedMemory),
                SizeClient.Humane(totalMemoryMapped));
        }

        private static bool CommandImportDir(List<string> args, RavenCli cli)
        {
            // ImportDir <databaseName> <path-to-dir>
            WriteText($"ImportDir for database {args[0]} from dir `{args[1]}` to {cli._server.WebUrl}", ConsoleColor.Yellow, cli);

            var url = $"{cli._server.WebUrl}/databases/{args[0]}/smuggler/import-dir?dir={args[1]}";
            using (var client = new HttpClient())
            {
                WriteText("Sending at " + DateTime.UtcNow, TextColor, cli);
                var result = client.GetAsync(url).Result;
                WriteText("At " + DateTime.UtcNow + " : Http Status Code = " + result.StatusCode, TextColor, cli);
            }
            WriteText("Http client closed.", TextColor, cli);
            return true;
        }

        private static bool CommandCreateDb(List<string> args, RavenCli cli)
        {
            // CreateDb <databaseName> <DataDir>
            WriteText($"Create database {args[0]} with DataDir `{args[1]}`", ConsoleColor.Yellow, cli);

            var port = new Uri(cli._server.WebUrl).Port;

            using (var store = new DocumentStore
            {
                Urls = new[] { $"http://127.0.0.1:{port}" },
                Database = args[0]
            }.Initialize())
            {
                var doc = new DatabaseRecord(args[0])
                {
                    Settings =
                    {
                        ["DataDir"] = args[1]
                    }
                };
                var res = store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc)).Result;
                WriteText("Database creation results = " + res.Name, TextColor, cli);
            }
            return true;
        }

        private static bool CommandPrint(List<string> args, RavenCli cli)
        {
            foreach (var arg in args)
                WriteText(">" + arg + "<", TextColor, cli);
            return true;
        }

        private static bool CommandHelp(List<string> args, RavenCli cli)
        {
            string[][] commandDescription = {
                new[] {"prompt <new prompt>", "Change the cli prompt. Can be used with variables. Type 'helpPrompt` for details"},
                new[] {"helpPrompt", "Detailed prompt command usage"},
                new[] {"clear", "Clear screen"},
                new[] {"stats", "Online server's memory consumption stats, request ratio and documents count"},
                new[] {"log [http-]<on|off|information/operations> [no-console]", "set log on/off or to specific mode. filter requests using http-on/offlog. no-console to avoid printing in CLI"},
                new[] {"info", "Print system info and current stats"},
                new[] {"logo [no-clear]", "Clear screen and print initial logo"},
                new[] {"gc [gen]", "Collect garbage of specified gen : 0, 1 or default 2"},
                new[] {"lowMem", "Simulate Low-Memory event"},
                new[] {"timer <on|off|fire>", "enable or disable candidate selection timer (Rachis), or fire timeout immediately"},
                new[] {"experimental <on|off>", "Set if to allow experimental cli commands. WARNING: Use with care!"},
                new[] {"script <server|database> [database]", "Execute script on server or specified database. WARNING: Use with care!"},
                new[] {"logout", "Logout (applicable only on piped connection)"},
                new[] {"resetServer", "Restarts the server (shutdown and re-run)"},
                new[] {"shutdown", "Shutdown the server"},
                new[] {"help", "This help screen"},
                new[] {"generateClientCert <name> <path-to-output-folder> [password]", "Generate a new trusted client certificate with 'ClusterAdmin' security clearance."},
                new[] {"trustServerCert <name> <path-to-pfx> [password]", "Register a server certificate of another node to be trusted on this server."},
                new[] {"trustClientCert <name> <path-to-pfx> [password]", "Register a client certificate to be trusted on this server with 'ClusterAdmin' security clearance."},
                new[] {"replaceClusterCert [-replaceImmediately] <name> <path-to-pfx> [password]", "Replace the cluster certificate."},
                new[] {"triggerCertificateRefresh [-replaceImmediately]", "Trigger a certificate refresh check (normally happens once an hour)."}
            };

            string[][] commandExperimentalDescription =
            {
                new[] {"createDb <database> <dir>", "Create database named 'database' in DataDir 'dir'"},
                new[] {"importDir <database> <path>", "Smuggler import entire directory (halts cli) from path"},
            };

            var msg = new StringBuilder("RavenDB CLI Help" + Environment.NewLine);
            msg.Append("================" + Environment.NewLine);
            msg.Append("Usage: <command> [args] [ && | || <command> [args] ] ..." + Environment.NewLine + Environment.NewLine);
            msg.Append("Commands:" + Environment.NewLine);

            WriteText(msg.ToString(), TextColor, cli);

            foreach (var cmd in commandDescription)
            {
                WriteText("\t" + cmd[0], ConsoleColor.Yellow, cli, newLine: false);
                WriteText(new string(' ', 73 - cmd[0].Length) + cmd[1], ConsoleColor.DarkYellow, cli);
            }
            WriteText("", TextColor, cli);

            if (cli._experimental)
            {
                msg.Append(Environment.NewLine + "Experimental Commands (WARNING: Use with care!):" + Environment.NewLine);
                foreach (var cmd in commandExperimentalDescription)
                {
                    WriteText("\t" + cmd[0], ConsoleColor.Yellow, cli, newLine: false);
                    WriteText(new string(' ', 74 - cmd[0].Length) + cmd[1], ConsoleColor.DarkYellow, cli);
                }
                WriteText("", TextColor, cli);
            }

            return true;
        }

        private readonly Dictionary<Command, SingleAction> _actions = new Dictionary<Command, SingleAction>
        {
            [Command.Prompt] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandPrompt },
            [Command.HelpPrompt] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandHelpPrompt },
            [Command.Stats] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandStats },
            [Command.Gc] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandGc },
            [Command.Log] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandLog },
            [Command.Clear] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandClear },
            [Command.TrustServerCert] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandTrustServerCert },
            [Command.TrustClientCert] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandTrustClientCert },
            [Command.GenerateClientCert] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandGenerateClientCert },
            [Command.ReplaceClusterCert] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandReplaceClusterCert},
            [Command.TriggerCertificateRefresh] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandTriggerCertificateRefresh},
            [Command.Info] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandInfo },
            [Command.Logo] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLogo },
            [Command.Experimental] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandExperimental },
            [Command.Script] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandScript },
            [Command.LowMem] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLowMem },
            [Command.Timer] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandTimer },
            [Command.ResetServer] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandResetServer },
            [Command.Logout] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandLogout },
            [Command.Shutdown] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandShutdown },
            [Command.Help] = new SingleAction { NumOfArgs = 0, DelegateFync = CommandHelp },

            // experimental, will not appear in 'help':
            [Command.ImportDir] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandImportDir, Experimental = true },
            [Command.CreateDb] = new SingleAction { NumOfArgs = 2, DelegateFync = CommandCreateDb, Experimental = true },
            [Command.Print] = new SingleAction { NumOfArgs = 1, DelegateFync = CommandPrint, Experimental = true }, // test cli
        };

        public bool Start(RavenServer server, TextWriter textWriter, TextReader textReader, bool consoleColoring)
        {
            _server = server;
            _writer = textWriter;
            _reader = textReader;
            _consoleColoring = consoleColoring;

            var parentProcessId = server.Configuration.Testing.ParentProcessId;
            if (parentProcessId != null)
            {
                void OnParentProcessExit(object o, EventArgs e)
                {
                    WriteText("Parent process " + parentProcessId + " has exited, closing",
                        ErrorColor, this);
                    Environment.Exit(-0xDEAD);
                }

                var parent = Process.GetProcessById(parentProcessId.Value);
                if (parent == null)
                {
                    OnParentProcessExit(this, EventArgs.Empty);
                    return false;
                }
                parent.EnableRaisingEvents = true;
                parent.Exited += OnParentProcessExit;
                if (parent.HasExited)
                    OnParentProcessExit(this, EventArgs.Empty);
            }

            try
            {
                return StartCli();
            }
            catch (Exception ex)
            {
                // incase of cli failure - prevent server from going down, and switch to a (very) simple fallback cli
                WriteText("\nERROR in CLI:" + ex, ErrorColor, this, newLine: false);
                WriteText("\n\nSwitching to simple cli...", ErrorColor, this, newLine: false);

                while (true)
                {
                    WriteText("(simple cli)>", TextColor, this, newLine: false);
                    var line = ReadLine(this);
                    if (line == null)
                        continue;
                    switch (line)
                    {
                        case "shutdown":
                        case "q":
                            return false;
                        case "reset":
                            return true;
                        case "log":
                            LoggingSource.Instance.EnableConsoleLogging();
                            LoggingSource.Instance.SetupLogMode(LogMode.Information, _server.Configuration.Logs.Path.FullPath);
                            break;
                        case "logoff":
                            LoggingSource.Instance.DisableConsoleLogging();
                            LoggingSource.Instance.SetupLogMode(LogMode.None, _server.Configuration.Logs.Path.FullPath);
                            break;
                        case "h":
                        case "help":
                            WriteText("Available commands: shutdown, reset, log, logoff", TextColor, this);
                            break;
                    }
                }
            }
        }

        private bool StartCli()
        {
            var ctrlCPressed = false;
            if (_consoleColoring)
                Console.CancelKeyPress += (sender, args) =>
                {
                    Console.ResetColor();
                    ctrlCPressed = true;
                };
            else
            {
                new WelcomeMessage(_writer).Print(); // beware not to print any delimiters until reaching PrintCliHeader
                _writer.WriteLine("Connected to RavenDB Console through named pipe connection..." + Environment.NewLine);
                _writer.Write(GetDelimiterString(Delimiter.ContinuePrinting));
                _writer.Flush();
            }
            while (true)
            {
                PrintCliHeader(this);
                var line = ReadLine(this);
                _writer.Flush();

                if (line == null)
                {
                    if (_consoleColoring == false)
                    {
                        // for some reason remote pipe couldn't ReadLine
                        WriteText("End of standard input detected. Remote console might not support input", ErrorColor, this);
                        // simulate logout:
                        line = "logout";
                    }
                    else
                    {
                        Thread.Sleep(75); //waiting for Ctrl+C 
                        if (ctrlCPressed)
                            break;
                        WriteText("End of standard input detected, switching to server mode...", WarningColor, this);

                        Program.RunAsNonInteractive();
                        return false;
                    }
                }

                var parsedLine = new ParsedLine { LineState = LineState.Begin };

                if (ParseLine(line, parsedLine) == false)
                {
                    WriteError(parsedLine.ErrorMsg, this);
                    continue;
                }

                if (parsedLine.LineState == LineState.Empty)
                    continue;

                var lastRc = true;
                foreach (var parsedCommand in parsedLine.ParsedCommands)
                {
                    if (lastRc == false)
                    {
                        if (parsedCommand.PrevConcatAction == ConcatAction.And)
                        {
                            WriteWarning($"Warning: Will not execute command `{parsedCommand.Command}` as previous command return non-successful return code", this);
                            break;
                        }
                        WriteWarning($"Warning: Will execute command `{parsedCommand.Command}` after previous command return non-successful return code", this);
                    }

                    if (_actions.ContainsKey(parsedCommand.Command) == false)
                    {
                        WriteError($"CLI Internal Error (missing definition for the command: {parsedCommand.Command})", this);
                        lastRc = false;
                        continue;
                    }

                    var cmd = _actions[parsedCommand.Command];

                    try
                    {
                        if (cmd.Experimental)
                        {
                            if (_experimental == false)
                            {
                                WriteError($"{parsedCommand.Command} is experimental, and can be executed only if expermintal option set to on", this);
                                lastRc = false;
                                continue;
                            }
                            WriteText("", TextColor, this);
                            WriteText("Are you sure you want to run experimental command : " + parsedCommand.Command + " ? [y/N] ", WarningColor, this, newLine: false);

                            var k = ReadKey(this);
                            WriteText("", TextColor, this);


                            if (char.ToLower(k).Equals('y') == false)
                            {
                                lastRc = false;
                                continue;
                            }
                        }
                        lastRc = cmd.DelegateFync.Invoke(parsedCommand.Args, this);

                        if (parsedCommand.Command == Command.Prompt && lastRc)
                            _promptArgs = parsedCommand.Args;
                        else if (parsedCommand.Command == Command.Experimental)
                        {
                            _experimental = lastRc;
                            lastRc = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        WriteError(ex.ToString(), this);
                        break;
                    }
                    if (lastRc)
                    {
                        if (parsedCommand.Command == Command.ResetServer)
                        {
                            if (Program.IsRunningNonInteractive || _writer == Console.Out)
                            {
                                if (_consoleColoring == false)
                                {
                                    const string str = "Restarting Server";
                                    PrintBothToConsoleAndRemotePipe(str, this, Delimiter.RestartServer);
                                }
                                return true;
                            }

                            WriteText("Server is not running as Service. Restarting from a remote connection is not allowed." + Environment.NewLine +
                                      "Please restart the server from its main console" + Environment.NewLine, WarningColor, this);
                        }
                        if (parsedCommand.Command == Command.Shutdown)
                        {
                            if (Program.IsRunningNonInteractive || _writer == Console.Out)
                            {
                                if (_consoleColoring == false)
                                {
                                    const string str = "Shutting down the server";
                                    PrintBothToConsoleAndRemotePipe(str, this, Delimiter.Shutdown);
                                }
                                return false;
                            }

                            WriteText("Server is not running as Service. Shutting down from a remote connection is not allowed." + Environment.NewLine +
                                      "Please shutdown the server from its main console" + Environment.NewLine, WarningColor, this);
                        }
                        if (parsedCommand.Command == Command.Logout)
                        {
                            break;
                        }
                    }
                    else
                    {
                        if (parsedCommand.Command == Command.Shutdown ||
                            parsedCommand.Command == Command.ResetServer)
                            lastRc = true; // if answered "No" for the above command - don't print ERROR
                    }
                }

                if (lastRc == false)
                {
                    WriteError("Command Failed", this);
                }
            }
            _writer.Flush();

            // we are logging out from cli
            Debug.Assert(_consoleColoring == false);
            return false;
        }

        private static void PrintBothToConsoleAndRemotePipe(string str, RavenCli cli, Delimiter delimiter)
        {
            cli._writer.WriteLine(str, TextColor, cli);
            cli._writer.Write(GetDelimiterString(delimiter));
            cli._writer.Flush();

            Console.ForegroundColor = WarningColor;
            Console.WriteLine();
            Console.WriteLine($"{str} from a remote pipe connection command");
            Console.WriteLine();
            Console.Out.Flush();
        }

        private static Command GetCommand(string fromWord)
        {
            if (char.IsNumber(fromWord[0]))
                return Command.UnknownCommand; // TryParse of enum returns true for numbers

            Command cmd = Command.UnknownCommand;
            var txt = fromWord.ToLower();
            if (Enum.TryParse(fromWord, true, out Command outText))
                return outText;

            switch (txt)
            {
                case "q":
                    cmd = Command.Shutdown;
                    break;
                case "h":
                    cmd = Command.Help;
                    break;
                case "cls":
                    cmd = Command.Clear;
                    break;
            }


            return cmd;
        }

        private bool ParseLine(string line, ParsedLine parsedLine, List<string> recursiveWords = null, ConcatAction? lastAction = null)
        {
            List<string> words;
            if (recursiveWords == null)
            {
                words = line.Split(new[] { ',', ' ' },
                    StringSplitOptions.RemoveEmptyEntries).ToList();

                if (words.Count == 0)
                {
                    parsedLine.LineState = LineState.Empty;
                    return true;
                }
            }
            else
            {
                words = recursiveWords;
            }

            if (parsedLine.LineState == LineState.Begin)
            {
                var cmd = GetCommand(words[0]);

                if (cmd == Command.UnknownCommand)
                {
                    parsedLine.ErrorMsg = $"Unknown command: `{words[0]}`";
                    return false;
                }

                ParsedCommand parsedCommand = new ParsedCommand { Command = cmd };
                parsedLine.ParsedCommands.Add(parsedCommand);
                parsedLine.LineState = LineState.AfterCommand;
                words.RemoveAt(0);
                if (lastAction != null)
                {
                    parsedLine.ParsedCommands.Last().PrevConcatAction = lastAction.Value;
                    lastAction = null;
                }
                if (words.Count == 0)
                {
                    if (_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs > 0)
                    {
                        parsedLine.ErrorMsg = $"Missing argument(s) after command : {parsedLine.ParsedCommands.Last().Command} (should get at least {_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs} arguments but got none)";
                        return false;
                    }
                    return true;
                }
            }

            if (parsedLine.LineState == LineState.AfterCommand)
            {
                var args = new List<string>();
                int i;
                for (i = 0; i < words.Count; i++)
                {
                    if (i == 0)
                    {
                        if (_actions.ContainsKey(parsedLine.ParsedCommands.Last().Command) == false)
                        {
                            parsedLine.ErrorMsg = $"Internal CLI Error : no definition for `{parsedLine.ParsedCommands.Last().Command}`";
                            return false;
                        }

                        switch (words[0])
                        {
                            case "&&":
                            case "||":
                                if (_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs != 0)
                                {
                                    parsedLine.ErrorMsg = $"Missing argument(s) after command : {parsedLine.ParsedCommands.Last().Command}";
                                    return false;
                                }
                                break;
                        }
                    }

                    if (words[i] != "&&" && words[i] != "||")
                    {
                        args.Add(words[i]);
                        continue;
                    }

                    if (words[i] == "&&")
                    {
                        parsedLine.LineState = LineState.AfterArgs;
                        lastAction = ConcatAction.And;
                        break;
                    }
                    if (words[i] == "||")
                    {
                        parsedLine.LineState = LineState.AfterArgs;
                        lastAction = ConcatAction.Or;
                        break;
                    }

                    // cannot reach here
                    parsedLine.ErrorMsg = "Internal CLI Error";
                    return false;
                }

                parsedLine.ParsedCommands.Last().Args = args;
                if (lastAction == null)
                {
                    if (args.Count < _actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs)
                    {
                        parsedLine.ErrorMsg = $"Missing argument(s) after command : {parsedLine.ParsedCommands.Last().Command} (should get at least {_actions[parsedLine.ParsedCommands.Last().Command].NumOfArgs} arguments but got {args.Count})";
                        return false;
                    }
                    return true;
                }

                List<string> newWords = new List<string>();
                for (int j = i + 1; j < words.Count; j++)
                {
                    newWords.Add(words[j]);
                }
                parsedLine.LineState = LineState.Begin;
                return ParseLine(null, parsedLine, newWords, lastAction);
            }

            return true;
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Commercial
{
    public class LicenseManager : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LicenseManager>("LicenseManager");
        private readonly LicenseStorage _licenseStorage = new LicenseStorage();
        private readonly LicenseStatus _licenseStatus = new LicenseStatus();
        private readonly BuildNumber _buildInfo;
        private Timer _leaseLicenseTimer;
        private bool _disableCalculatingLicenseLimits;
        private RSAParameters? _rsaParameters;
        private readonly ServerStore _serverStore;
        private readonly SemaphoreSlim _leaseLicenseSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _licenseLimitsSemaphore = new SemaphoreSlim(1);
        private readonly bool _skipLeasingErrorsLogging;
        private DateTime? _lastPerformanceHint;
        private bool _eulaAcceptedButHasPendingRestart;

        private readonly object _locker = new object();

        public event Action LicenseChanged;

        public LicenseManager(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _skipLeasingErrorsLogging = serverStore.Configuration.Licensing.SkipLeasingErrorsLogging;

            _buildInfo = new BuildNumber
            {
                BuildVersion = ServerVersion.Build,
                ProductVersion = ServerVersion.Version,
                CommitHash = ServerVersion.CommitHash,
                FullVersion = ServerVersion.FullVersion
            };
        }

        public bool IsEulaAccepted => _eulaAcceptedButHasPendingRestart || _serverStore.Configuration.Licensing.EulaAccepted;

        private RSAParameters RSAParameters
        {
            get
            {
                if (_rsaParameters != null)
                    return _rsaParameters.Value;

                string publicKeyString;
                const string publicKeyPath = "Raven.Server.Commercial.RavenDB.public.json";
                using (var stream = typeof(LicenseManager).GetTypeInfo().Assembly.GetManifestResourceStream(publicKeyPath))
                {
                    if (stream == null)
                        throw new InvalidOperationException("Could not find public key for the license");
                    publicKeyString = new StreamReader(stream).ReadToEnd();
                }

                var rsaPublicParameters = JsonConvert.DeserializeObject<RSAPublicParameters>(publicKeyString);
                _rsaParameters = new RSAParameters
                {
                    Modulus = rsaPublicParameters.RsaKeyValue.Modulus,
                    Exponent = rsaPublicParameters.RsaKeyValue.Exponent
                };
                return _rsaParameters.Value;
            }
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            try
            {
                _licenseStorage.Initialize(environment, contextPool);

                var firstServerStartDate = _licenseStorage.GetFirstServerStartDate();
                if (firstServerStartDate == null)
                {
                    firstServerStartDate = SystemTime.UtcNow;
                    _licenseStorage.SetFirstServerStartDate(firstServerStartDate.Value);
                }

                _licenseStatus.FirstServerStartDate = firstServerStartDate.Value;

                ReloadLicense(addPerformanceHint: true);
                AsyncHelpers.RunSync(() => CalculateLicenseLimits());
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to initialize license manager", e);
            }
            finally
            {
                _leaseLicenseTimer = new Timer(state =>
                    AsyncHelpers.RunSync(ExecuteTasks), null,
                    (int)TimeSpan.FromMinutes(1).TotalMilliseconds,
                    (int)TimeSpan.FromHours(24).TotalMilliseconds);
            }
        }

        public LicenseStatus GetLicenseStatus()
        {
            return _licenseStatus;
        }

        public void ReloadLicense(bool addPerformanceHint = false)
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
            {
                // license is not active
                _licenseStatus.Attributes = null;
                _licenseStatus.ErrorMessage = null;
                _licenseStatus.Id = null;
                return;
            }

            try
            {
                _licenseStatus.Attributes = LicenseValidator.Validate(license, RSAParameters);
                _licenseStatus.ErrorMessage = null;
                _licenseStatus.Id = license.Id;
            }
            catch (Exception e)
            {
                _licenseStatus.Attributes = null;
                _licenseStatus.ErrorMessage = e.Message;
                _licenseStatus.Id = null;

                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not validate license", e);

                var alert = AlertRaised.Create(
                     null,
                    "License manager initialization error",
                    "Could not initialize the license manager",
                    AlertType.LicenseManager_InitializationError,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);
            }

            LicenseChanged?.Invoke();

            ReloadLicenseLimits(addPerformanceHint);
        }

        public void ReloadLicenseLimits(bool addPerformanceHint = false)
        {
            try
            {
                var licenseLimits = _serverStore.LoadLicenseLimits();

                if (licenseLimits?.NodeLicenseDetails != null &&
                    licenseLimits.NodeLicenseDetails.TryGetValue(_serverStore.NodeTag, out var detailsPerNode))
                {
                    var cores = Math.Min(detailsPerNode.UtilizedCores, _licenseStatus.MaxCores);

                    using (var process = Process.GetCurrentProcess())
                    {
                        SetAffinity(process, cores, addPerformanceHint);

                        var ratio = (int)Math.Ceiling(_licenseStatus.MaxMemory / (double)_licenseStatus.MaxCores);
                        var clusterSize = GetClusterSize();
                        var maxWorkingSet = Math.Min(_licenseStatus.MaxMemory / (double)clusterSize, cores * ratio);
                        SetMaxWorkingSet(process, Math.Max(1, maxWorkingSet));
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Info("Failed to reload license limits", e);
            }
        }

        private int GetClusterSize()
        {
            if (_serverStore.IsPassive())
                return 1;

            return _serverStore.GetClusterTopology().AllNodes.Count;
        }

        public async Task ChangeLicenseLimits(string nodeTag, int newAssignedCores)
        {
            if (_serverStore.IsLeader() == false)
                throw new InvalidOperationException("Only the leader is allowed to change the license limits");

            if (_licenseLimitsSemaphore.Wait(1000 * 10) == false)
                throw new TimeoutException("License limit change is already in progress. " +
                                           "Please try again later.");

            try
            {
                var licenseLimits = GetOrCreateLicenseLimits();

                var oldAssignedCores = 0;
                var numberOfCores = -1;
                double installedMemoryInGb = -1;
                double usableMemoryInGb = -1;
                if (licenseLimits.NodeLicenseDetails.TryGetValue(nodeTag, out var nodeDetails))
                {
                    installedMemoryInGb = nodeDetails.InstalledMemoryInGb;
                    usableMemoryInGb = nodeDetails.UsableMemoryInGb;
                    oldAssignedCores = nodeDetails.UtilizedCores;
                }

                var utilizedCores = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores) - oldAssignedCores + newAssignedCores;
                var maxCores = _licenseStatus.MaxCores;
                if (utilizedCores > maxCores)
                {
                    var message = $"Cannot change the license limit for node {nodeTag} " +
                                  $"from {oldAssignedCores} core{Pluralize(oldAssignedCores)} " +
                                  $"to {newAssignedCores} core{Pluralize(newAssignedCores)} " +
                                  $"because the utilized number of cores in the cluster will be {utilizedCores} " +
                                  $"while the maximum allowed cores according to the license is {maxCores}.";
                    throw new LicenseLimitException(LimitType.Cores, message);
                }

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var allNodes = _serverStore.GetClusterTopology(context).AllNodes;
                    if (allNodes.TryGetValue(nodeTag, out var nodeUrl) == false)
                        throw new ArgumentException($"Node tag: {nodeTag} isn't part of the cluster");

                    if (nodeTag == _serverStore.NodeTag)
                    {
                        numberOfCores = ProcessorInfo.ProcessorCount;
                        var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
                        installedMemoryInGb = memoryInfo.InstalledMemory;
                        usableMemoryInGb = memoryInfo.UsableMemory;
                    }
                    else
                    {
                        var nodeInfo = await GetNodeInfo(nodeUrl, context);
                        if (nodeInfo == null && numberOfCores == -1)
                        {
                            throw new InvalidOperationException($"Node tag: {nodeTag} with node url: {nodeUrl} cannot be reached");
                        }
                        else if (nodeInfo != null)
                        {
                            numberOfCores = nodeInfo.NumberOfCores;
                            installedMemoryInGb = nodeInfo.InstalledMemoryInGb;
                            usableMemoryInGb = nodeInfo.UsableMemoryInGb;
                        }
                    }

                    Debug.Assert(numberOfCores > 0);

                    if (numberOfCores < newAssignedCores)
                        throw new ArgumentException($"The new assigned cores count: {newAssignedCores} " +
                                                    $"is larger than the number of cores in the node: {numberOfCores}");

                    licenseLimits.NodeLicenseDetails[nodeTag] = new DetailsPerNode
                    {
                        UtilizedCores = newAssignedCores,
                        NumberOfCores = numberOfCores,
                        InstalledMemoryInGb = installedMemoryInGb,
                        UsableMemoryInGb = usableMemoryInGb
                    };
                }

                await _serverStore.PutLicenseLimitsAsync(licenseLimits);
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
        }

        private LicenseLimits GetOrCreateLicenseLimits()
        {
            var licenseLimits = _serverStore.LoadLicenseLimits() ?? new LicenseLimits();
            if (licenseLimits.NodeLicenseDetails == null)
            {
                licenseLimits.NodeLicenseDetails = new Dictionary<string, DetailsPerNode>();
            }

            return licenseLimits;
        }

        public async Task CalculateLicenseLimits(
            NodeDetails newNodeDetails = null,
            bool forceFetchingNodeInfo = false,
            bool waitToUpdate = false)
        {
            if (_serverStore.IsLeader() == false)
                return;

            if (_disableCalculatingLicenseLimits)
                return;

            if (_licenseLimitsSemaphore.Wait(waitToUpdate ? 1000 : 0) == false)
                return;

            try
            {
                var licenseLimits = await UpdateNodesInfoInternal(forceFetchingNodeInfo, newNodeDetails);
                if (licenseLimits == null)
                    return;

                _serverStore.PutLicenseLimits(licenseLimits);
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to calculate license limits", e);
            }
            finally
            {
                _licenseLimitsSemaphore.Release();
            }
        }

        private int GetUtilizedCores()
        {
            var licenseLimits = _serverStore.LoadLicenseLimits();
            var detailsPerNode = licenseLimits?.NodeLicenseDetails;
            if (detailsPerNode == null)
                return 0;

            return detailsPerNode.Sum(x => x.Value.UtilizedCores);
        }

        public int GetCoresToAssign(int numberOfCores)
        {
            var utilizedCores = GetUtilizedCores();
            if (utilizedCores == 0)
                return Math.Min(numberOfCores, _licenseStatus.MaxCores);

            var availableCores = Math.Max(1, _licenseStatus.MaxCores - utilizedCores);
            return Math.Min(availableCores, numberOfCores);
        }

        private void AssertCanAssignCores(int assignedCores)
        {
            var licenseLimits = _serverStore.LoadLicenseLimits();
            if (licenseLimits?.NodeLicenseDetails == null ||
                licenseLimits.NodeLicenseDetails.Count == 0)
                return;

            var coresInUse = licenseLimits.NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);
            if (coresInUse + assignedCores > _licenseStatus.MaxCores)
            {
                var message = $"Can't assign {assignedCores} core{Pluralize(assignedCores)} " +
                          $"to the node, max allowed cores on license: {_licenseStatus.MaxCores}, " +
                          $"number of utilized cores: {coresInUse}";
                throw GenerateLicenseLimit(LimitType.Cores, message);
            }
        }

        public async Task Activate(License license, bool skipLeaseLicense, bool ensureNotPassive = true)
        {
            var newLicenseStatus = GetLicenseStatus(license);

            var licenseExpiration = newLicenseStatus.Expiration;
            if (licenseExpiration.HasValue == false)
                throw new LicenseExpiredException("License doesn't have an expiration date!");

            if (licenseExpiration < DateTime.UtcNow)
            {
                if (skipLeaseLicense == false)
                {
                    // license expired, we'll try to update it
                    license = await GetUpdatedLicenseInternal(license);
                    if (license != null)
                    {
                        await Activate(license, skipLeaseLicense: true, ensureNotPassive: ensureNotPassive);
                        return;
                    }
                }

                throw new LicenseExpiredException($"License already expired on: {licenseExpiration}");
            }

            ThrowIfCannotActivateLicense(newLicenseStatus);

            using (DisableCalculatingCoresCount())
            {
                if (ensureNotPassive)
                    _serverStore.EnsureNotPassive();

                try
                {

                    await _serverStore.PutLicenseAsync(license);

                    _licenseStatus.Attributes = newLicenseStatus.Attributes;
                    _licenseStatus.ErrorMessage = null;
                    _licenseStatus.Id = license.Id;
                }
                catch (Exception e)
                {
                    var message = $"Could not save the following license:{Environment.NewLine}" +
                                  $"Id: {license.Id}{Environment.NewLine}" +
                                  $"Name: {license.Name}{Environment.NewLine}" +
                                  $"Keys: [{(license.Keys != null ? string.Join(", ", license.Keys) : "N/A")}]";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(message, e);

                    throw new InvalidDataException("Could not save license!", e);
                }
            }

            await CalculateLicenseLimits(forceFetchingNodeInfo: true, waitToUpdate: true);
        }

        public LicenseStatus GetLicenseStatus(License license)
        {
            Dictionary<string, object> licenseAttributes;

            try
            {
                licenseAttributes = LicenseValidator.Validate(license, RSAParameters);
            }
            catch (Exception e)
            {
                var message = $"Could not validate the following license:{Environment.NewLine}" +
                              $"Id: {license.Id}{Environment.NewLine}" +
                              $"Name: {license.Name}{Environment.NewLine}" +
                              $"Keys: [{(license.Keys != null ? string.Join(", ", license.Keys) : "N/A")}]";

                if (Logger.IsInfoEnabled)
                    Logger.Info(message, e);

                throw new InvalidDataException("Could not validate license!", e);
            }

            var newLicenseStatus = new LicenseStatus
            {
                Attributes = licenseAttributes
            };
            return newLicenseStatus;
        }

        private IDisposable DisableCalculatingCoresCount()
        {
            _disableCalculatingLicenseLimits = true;
            return new DisposableAction(() =>
            {
                _disableCalculatingLicenseLimits = false;
            });
        }

        public void TryActivateLicense()
        {
            if (_licenseStatus.Type != LicenseType.None)
                return;

            var license = TryGetLicenseFromString() ?? TryGetLicenseFromPath();
            if (license == null)
                return;

            try
            {
                AsyncHelpers.RunSync(() => Activate(license, skipLeaseLicense: false, ensureNotPassive: false));
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to activate license", e);
            }
        }

        private License TryGetLicenseFromPath()
        {
            var path = _serverStore.Configuration.Licensing.LicensePath;

            try
            {
                using (var stream = File.Open(path.FullPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    return DeserializeLicense(stream);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read license from '{path.FullPath}' path", e);
            }

            return null;
        }

        private License TryGetLicenseFromString()
        {
            var licenseString = _serverStore.Configuration.Licensing.License;
            if (string.IsNullOrWhiteSpace(licenseString))
                return null;

            try
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(licenseString)))
                {
                    return DeserializeLicense(stream);
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to read license from '{RavenConfiguration.GetKey(x => x.Licensing.License)}' configuration", e);
            }

            return null;
        }

        private static License DeserializeLicense(Stream stream)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(stream, "license/json");
                return JsonDeserializationServer.License(json);
            }
        }

        public async Task<License> GetUpdatedLicense(
            License currentLicense, 
            Func<HttpResponseMessage, Task> onFailure = null, 
            Func<LeasedLicense, License> onSuccess = null)
        {
            var leaseLicenseInfo = new LeaseLicenseInfo
            {
                License = currentLicense,
                BuildInfo = _buildInfo,
                ClusterId = _serverStore.GetClusterTopology().TopologyId,
                UtilizedCores = GetUtilizedCores()
            };

            var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/lease",
                    new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                if (onFailure != null)
                {
                    await onFailure(response).ConfigureAwait(false);
                }
                
                return null;
            }

            var leasedLicenseAsStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(leasedLicenseAsStream, "leased license info");
                var leasedLicense = JsonDeserializationServer.LeasedLicense(json);

                if (onSuccess == null)
                    return leasedLicense.License;

                return onSuccess.Invoke(leasedLicense);
            }
        }

        private async Task<License> GetUpdatedLicenseInternal(License currentLicense)
        {
            return await GetUpdatedLicense(currentLicense,
                    async response => await HandleLeaseLicenseFailure(response).ConfigureAwait(false),
                    leasedLicense =>
                    {
                        var newLicense = leasedLicense.License;
                        var licenseChanged =
                            newLicense.Name != currentLicense.Name ||
                            newLicense.Id != currentLicense.Id ||
                            newLicense.Keys.All(currentLicense.Keys.Contains) == false;

                        if (string.IsNullOrWhiteSpace(leasedLicense.Message) == false)
                        {
                            var severity =
                                leasedLicense.NotificationSeverity == NotificationSeverity.None 
                                    ? NotificationSeverity.Info : leasedLicense.NotificationSeverity;
                            var alert = AlertRaised.Create(
                                null,
                                leasedLicense.Title,
                                leasedLicense.Message,
                                AlertType.LicenseManager_LicenseUpdateMessage,
                                severity);

                            _serverStore.NotificationCenter.Add(alert);
                        }

                        if (string.IsNullOrWhiteSpace(leasedLicense.ErrorMessage) == false)
                        {
                            _licenseStatus.ErrorMessage = leasedLicense.ErrorMessage;
                        }

                        return licenseChanged ? leasedLicense.License : null;
                    })
                .ConfigureAwait(false);
        }

        private async Task ExecuteTasks()
        {
            try
            {
                await LeaseLicense();

                await CalculateLicenseLimits(forceFetchingNodeInfo: true, waitToUpdate: true);

                ReloadLicenseLimits(addPerformanceHint: true);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to execute tasks", e);
            }
        }

        public async Task LeaseLicense(bool forceUpdate = false)
        {
            if (forceUpdate == false && _serverStore.IsLeader() == false)
                return;

            if (_leaseLicenseSemaphore.Wait(0) == false)
                return;

            try
            {
                var loadedLicense = _serverStore.LoadLicense();
                if (loadedLicense == null)
                    return;

                var updatedLicense = await GetUpdatedLicenseInternal(loadedLicense);
                if (updatedLicense == null)
                    return;

                await Activate(updatedLicense, skipLeaseLicense: true);

                var alert = AlertRaised.Create(
                    null,
                    "License was updated",
                    "Successfully leased license",
                    AlertType.LicenseManager_LeaseLicenseSuccess,
                    NotificationSeverity.Info);

                _serverStore.NotificationCenter.Add(alert);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to lease license", e);

                var alert = AlertRaised.Create(
                    null,
                    "Failed to lease license",
                    "Could not lease license",
                    AlertType.LicenseManager_LeaseLicenseError,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e));

                _serverStore.NotificationCenter.Add(alert);

                if (forceUpdate)
                    throw;
            }
            finally
            {
                _leaseLicenseSemaphore.Release();
            }
        }

        private async Task<LicenseLimits> UpdateNodesInfoInternal(
            bool forceFetchingNodeInfo,
            NodeDetails newNodeDetails)
        {
            var licenseLimits = GetOrCreateLicenseLimits();
            var hasChanges = false;

            var detailsPerNode = licenseLimits.NodeLicenseDetails;
            if (newNodeDetails != null)
            {
                hasChanges = true;
                detailsPerNode[newNodeDetails.NodeTag] = new DetailsPerNode
                {
                    UtilizedCores = newNodeDetails.AssignedCores,
                    NumberOfCores = newNodeDetails.NumberOfCores,
                    InstalledMemoryInGb = newNodeDetails.InstalledMemoryInGb,
                    UsableMemoryInGb = newNodeDetails.UsableMemoryInGb
                };
            }

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var allNodes = _serverStore.GetClusterTopology(context).AllNodes;
                var missingNodesAssignment = new List<string>();

                foreach (var node in allNodes)
                {
                    int numberOfCores;
                    double installedMemoryInGb;
                    double usableMemoryInGb;
                    var nodeTag = node.Key;
                    if (nodeTag == _serverStore.NodeTag)
                    {
                        numberOfCores = ProcessorInfo.ProcessorCount;
                        var memoryInfo = MemoryInformation.GetMemoryInfoInGb();
                        installedMemoryInGb = memoryInfo.InstalledMemory;
                        usableMemoryInGb = memoryInfo.UsableMemory;
                    }
                    else
                    {
                        if (forceFetchingNodeInfo == false)
                            continue;

                        var nodeInfo = await GetNodeInfo(node.Value, context);
                        if (nodeInfo == null)
                            continue;

                        numberOfCores = nodeInfo.NumberOfCores;
                        installedMemoryInGb = nodeInfo.InstalledMemoryInGb;
                        usableMemoryInGb = nodeInfo.UsableMemoryInGb;
                    }

                    var nodeDetailsExist = detailsPerNode.TryGetValue(nodeTag, out var nodeDetails);
                    if (nodeDetailsExist &&
                        nodeDetails.NumberOfCores == numberOfCores &&
                        nodeDetails.UsableMemoryInGb.Equals(usableMemoryInGb) &&
                        nodeDetails.InstalledMemoryInGb.Equals(installedMemoryInGb))
                    {
                        // nodes hardware didn't change
                        continue;
                    }

                    hasChanges = true;
                    detailsPerNode[nodeTag] = new DetailsPerNode
                    {
                        NumberOfCores = numberOfCores,
                        InstalledMemoryInGb = installedMemoryInGb,
                        UsableMemoryInGb = usableMemoryInGb
                    };

                    if (nodeDetailsExist == false)
                    {
                        // we'll assign the utilized cores later
                        missingNodesAssignment.Add(nodeTag);
                        continue;
                    }

                    detailsPerNode[nodeTag].UtilizedCores = Math.Min(numberOfCores, nodeDetails.UtilizedCores);
                }

                var nodesToRemove = detailsPerNode.Keys.Except(allNodes.Keys).ToList();
                foreach (var nodeToRemove in nodesToRemove)
                {
                    hasChanges = true;
                    detailsPerNode.Remove(nodeToRemove);
                }

                AssignMissingNodeCores(missingNodesAssignment, detailsPerNode);
                VerifyCoresPerNode(detailsPerNode, ref hasChanges);
                ValidateLicenseStatus(detailsPerNode);
            }

            return hasChanges ? licenseLimits : null;
        }

        private void ValidateLicenseStatus(Dictionary<string, DetailsPerNode> detailsPerNode)
        {
            var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);
            string errorMessage = null;
            if (utilizedCores > _licenseStatus.MaxCores)
            {
                errorMessage = $"The number of utilized cores is {utilizedCores}, " +
                              $"while the license limit is {_licenseStatus.MaxCores} cores";
            }
            else if (detailsPerNode.Count > _licenseStatus.MaxClusterSize)
            {
                errorMessage = $"The cluster size is {detailsPerNode.Count}, " +
                              $"while the license limit is {_licenseStatus.MaxClusterSize}";
            }

            if (errorMessage != null)
                _licenseStatus.ErrorMessage = errorMessage;
        }

        private void VerifyCoresPerNode(Dictionary<string, DetailsPerNode> detailsPerNode, ref bool hasChanges)
        {
            var maxCores = _licenseStatus.MaxCores;
            var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);

            if (maxCores >= utilizedCores)
                return;

            hasChanges = true;
            var coresPerNode = Math.Max(1, maxCores / detailsPerNode.Count);
            foreach (var nodeDetails in detailsPerNode)
            {
                nodeDetails.Value.UtilizedCores = coresPerNode;
            }
        }

        private void AssignMissingNodeCores(
            List<string> missingNodesAssignment,
            Dictionary<string, DetailsPerNode> detailsPerNode)
        {
            if (missingNodesAssignment.Count == 0)
                return;

            var utilizedCores = detailsPerNode.Sum(x => x.Value.UtilizedCores);
            var availableCores = _licenseStatus.MaxCores - utilizedCores;
            var coresPerNode = Math.Max(1, availableCores / missingNodesAssignment.Count);

            foreach (var nodeTag in missingNodesAssignment)
            {
                if (detailsPerNode.TryGetValue(nodeTag, out var nodeDetails) == false)
                    continue;

                coresPerNode = Math.Min(coresPerNode, nodeDetails.NumberOfCores);

                nodeDetails.UtilizedCores = coresPerNode;
            }
        }

        private async Task<NodeInfo> GetNodeInfo(string nodeUrl, TransactionOperationContext ctx)
        {
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(nodeUrl, _serverStore.Server.Certificate.Certificate))
            {
                var infoCmd = new GetNodeInfoCommand();

                try
                {
                    await requestExecutor.ExecuteAsync(infoCmd, ctx);
                }
                catch (Exception)
                {
                    return null;
                }

                return infoCmd.Result;
            }
        }

        private async Task HandleLeaseLicenseFailure(HttpResponseMessage response)
        {
            if (Logger.IsInfoEnabled == false || _skipLeasingErrorsLogging)
                return;

            var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

            var alert = AlertRaised.Create(
                null,
                "Lease license failure",
                "Could not lease license",
                AlertType.LicenseManager_LeaseLicenseError,
                NotificationSeverity.Warning,
                details: new ExceptionDetails(
                    new InvalidOperationException($"Status code: {response.StatusCode}, response: {responseString}")));

            _serverStore.NotificationCenter.Add(alert);
        }

        private void SetAffinity(Process process, int cores, bool addPerformanceHint)
        {
            if (cores > ProcessorInfo.ProcessorCount)
            {
                // the number of assigned cores is larger than we have on this machine
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Cannot set affinity to {cores} core(s), " +
                                $"when the number of cores on this machine is: {ProcessorInfo.ProcessorCount}");
                return;
            }

            try
            {
                var currentlyAssignedCores = Bits.NumberOfSetBits(process.ProcessorAffinity.ToInt64());
                if (currentlyAssignedCores == cores &&
                    _lastPerformanceHint != null &&
                    _lastPerformanceHint.Value.AddDays(7) > DateTime.UtcNow)
                {
                    // we already set the correct number of assigned cores
                    return;
                }

                var bitMask = 1L;
                var processAffinityMask = _serverStore.Configuration.Server.ProcessAffinityMask;
                if (processAffinityMask == null)
                {
                    for (var i = 0; i < cores; i++)
                    {
                        bitMask |= 1L << i;
                    }
                }
                else if (Bits.NumberOfSetBits(processAffinityMask.Value) > cores)
                {
                    var affinityMask = processAffinityMask.Value;
                    var bitNumber = 0;
                    while (cores > 0)
                    {
                        if ((affinityMask & 1) != 0)
                        {
                            bitMask |= 1L << bitNumber;
                            cores--;
                        }

                        affinityMask = affinityMask >> 1;
                        bitNumber++;
                    }
                }
                else
                {
                    bitMask = processAffinityMask.Value;
                }

                process.ProcessorAffinity = new IntPtr(bitMask);

                if (addPerformanceHint &&
                    ProcessorInfo.ProcessorCount > cores)
                {
                    _lastPerformanceHint = DateTime.UtcNow;
                    var notification = PerformanceHint.Create(
                        null,
                        "Your database can be faster - not all cores are used",
                        $"Your server is currently using only {cores} core{Pluralize(cores)} " +
                        $"out of the {Environment.ProcessorCount} that it has available",
                        PerformanceHintType.UnusedCapacity,
                        NotificationSeverity.Info,
                        "LicenseManager");
                    _serverStore.NotificationCenter.Add(notification);
                }
            }
            catch (PlatformNotSupportedException)
            {
                // nothing to do
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set affinity for {cores} cores, error code: {Marshal.GetLastWin32Error()}", e);
            }
        }

       
        private static void SetMaxWorkingSet(Process process, double ramInGb)
        {
            try
            {
                MemoryExtensions.SetWorkingSet(process, ramInGb, Logger);
            }
            catch (Exception e)
            {
                Logger.Info($"Failed to set max working set to {ramInGb}GB, error code: {Marshal.GetLastWin32Error()}", e);
            }
        }

        public void Dispose()
        {
            _leaseLicenseTimer?.Dispose();
        }

        private void ThrowIfCannotActivateLicense(LicenseStatus newLicenseStatus)
        {
            var clusterSize = GetClusterSize();
            var maxClusterSize = newLicenseStatus.MaxClusterSize;
            if (clusterSize > maxClusterSize)
            {
                var message = "Cannot activate license because the maximum allowed cluster size is: " +
                              $"{maxClusterSize} while the current cluster size is: {clusterSize}";
                throw GenerateLicenseLimit(LimitType.ClusterSize, message);
            }

            var maxCores = newLicenseStatus.MaxCores;
            if (clusterSize > maxCores)
            {
                var message = "Cannot activate license because the cores limit is: " +
                              $"{maxCores} while the current cluster size is: {clusterSize}!";
                throw GenerateLicenseLimit(LimitType.Cores, message);
            }

            var encryptedDatabasesCount = 0;
            var externalReplicationCount = 0;
            var delayedExternalReplicationCount = 0;
            var ravenEtlCount = 0;
            var sqlEtlCount = 0;
            var snapshotBackupsCount = 0;
            var cloudBackupsCount = 0;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var databaseRecord in _serverStore.Cluster.ReadAllDatabases(context))
                {
                    if (databaseRecord.Encrypted)
                        encryptedDatabasesCount++;

                    if (databaseRecord.ExternalReplications != null &&
                        databaseRecord.ExternalReplications.Count > 0)
                        externalReplicationCount++;

                    if (databaseRecord.ExternalReplications != null &&
                        databaseRecord.ExternalReplications.Count(x => x.DelayReplicationFor != TimeSpan.Zero) > 0)
                        delayedExternalReplicationCount++;

                    if (HasRavenEtl(databaseRecord.RavenEtls,
                        databaseRecord.RavenConnectionStrings))
                        ravenEtlCount++;

                    if (databaseRecord.SqlEtls != null &&
                        databaseRecord.SqlEtls.Count > 0)
                        sqlEtlCount++;

                    var backupTypes = GetBackupTypes(databaseRecord.PeriodicBackups);
                    if (backupTypes.HasSnapshotBackup)
                        snapshotBackupsCount++;
                    if (backupTypes.HasCloudBackup)
                        cloudBackupsCount++;
                }
            }

            if (encryptedDatabasesCount > 0 &&
                newLicenseStatus.HasEncryption == false)
            {
                var message = GenerateDetails(encryptedDatabasesCount, "encryption");
                throw GenerateLicenseLimit(LimitType.Encryption, message);
            }

            if (externalReplicationCount > 0 &&
                newLicenseStatus.HasExternalReplication == false)
            {
                var message = GenerateDetails(externalReplicationCount, "external replication");
                throw GenerateLicenseLimit(LimitType.ExternalReplication, message);
            }

            if (delayedExternalReplicationCount > 0 &&
                newLicenseStatus.HasDelayedExternalReplication == false)
            {
                var message = GenerateDetails(externalReplicationCount, "delayed external replication");
                throw GenerateLicenseLimit(LimitType.DelayedExternalReplication, message);
            }

            if (ravenEtlCount > 0 &&
                newLicenseStatus.HasRavenEtl == false)
            {
                var message = GenerateDetails(ravenEtlCount, "Raven ETL");
                throw GenerateLicenseLimit(LimitType.RavenEtl, message);
            }

            if (sqlEtlCount > 0 &&
                newLicenseStatus.HasSqlEtl == false)
            {
                var message = GenerateDetails(sqlEtlCount, "SQL ETL");
                throw GenerateLicenseLimit(LimitType.SqlEtl, message);
            }

            if (snapshotBackupsCount > 0 &&
                newLicenseStatus.HasSnapshotBackups == false)
            {
                var message = GenerateDetails(snapshotBackupsCount, "snapshot backups");
                throw GenerateLicenseLimit(LimitType.SnapshotBackup, message);
            }

            if (cloudBackupsCount > 0 &&
                newLicenseStatus.HasCloudBackups == false)
            {
                var message = GenerateDetails(cloudBackupsCount, "cloud backups");
                throw GenerateLicenseLimit(LimitType.CloudBackup, message);
            }
        }

        private static string GenerateDetails(int count, string feature)
        {
            return $"Cannot activate license because there {(count == 1 ? "is" : "are")} " +
                   $"{count} database{Pluralize(count)} that use{(count == 1 ? "s" : string.Empty)} {feature} " +
                   $"while the new license doesn't include the usage of {feature}";
        }

        private static string Pluralize(int count)
        {
            return count == 1 ? string.Empty : "s";
        }

        private static bool HasRavenEtl(List<RavenEtlConfiguration> ravenEtls,
            Dictionary<string, RavenConnectionString> ravenConnectionStrings)
        {
            if (ravenEtls == null)
                return false;

            foreach (var ravenEtl in ravenEtls)
            {
                if (ravenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var _) == false)
                    continue;

                return true;
            }

            return false;
        }

        private static (bool HasSnapshotBackup, bool HasCloudBackup) GetBackupTypes(
            IEnumerable<PeriodicBackupConfiguration> periodicBackups)
        {
            var hasSnapshotBackup = false;
            var hasCloudBackup = false;
            foreach (var configuration in periodicBackups)
            {
                hasSnapshotBackup |= configuration.BackupType == BackupType.Snapshot;
                hasCloudBackup |= configuration.HasCloudBackup();

                if (hasSnapshotBackup && hasCloudBackup)
                    return (true, true);
            }

            return (hasSnapshotBackup, hasCloudBackup);
        }

        public void AssertCanAddNode(string nodeUrl, int assignedCores)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            AssertCanAssignCores(assignedCores);

            if (_licenseStatus.DistributedCluster == false)
            {
                var message = $"Your current license ({_licenseStatus.Type}) does not allow adding nodes to the cluster";
                throw GenerateLicenseLimit(LimitType.ForbiddenHost, message);
            }

            var maxClusterSize = _licenseStatus.MaxClusterSize;
            var clusterSize = GetClusterSize();
            if (++clusterSize > maxClusterSize)
            {
                var message = $"Your current license allows up to {maxClusterSize} nodes in a cluster";
                throw GenerateLicenseLimit(LimitType.ClusterSize, message);
            }
        }

        public void AssertCanAddPeriodicBackup(BlittableJsonReaderObject readerObject)
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var backupBlittable = readerObject.Clone(context);
                var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(backupBlittable);
                if (configuration.BackupType == BackupType.Snapshot &&
                    _licenseStatus.HasSnapshotBackups == false)
                {
                    const string details = "Your current license doesn't include the snapshot backups feature";
                    throw GenerateLicenseLimit(LimitType.SnapshotBackup, details);
                }

                var hasCloudBackup = configuration.HasCloudBackup();
                if (hasCloudBackup && _licenseStatus.HasCloudBackups == false)
                {
                    const string details = "Your current license doesn't include the backup to cloud or ftp feature!";
                    throw GenerateLicenseLimit(LimitType.CloudBackup, details);
                }
            }
        }

        public void AssertCanAddExternalReplication()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (_licenseStatus.HasExternalReplication)
                return;

            var details = $"Your current license ({_licenseStatus.Type}) does not allow adding external replication";
            throw GenerateLicenseLimit(LimitType.ExternalReplication, details);
        }

        public void AssertCanAddRavenEtl()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (_licenseStatus.HasRavenEtl)
                return;

            const string message = "Your current license doesn't include the RavenDB ETL feature";
            throw GenerateLicenseLimit(LimitType.RavenEtl, message);
        }

        public void AssertCanAddSqlEtl()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (_licenseStatus.HasSqlEtl != false)
                return;

            const string message = "Your current license doesn't include the SQL ETL feature";
            throw GenerateLicenseLimit(LimitType.SqlEtl, message);
        }

        public bool CanUseSnmpMonitoring()
        {
            if (IsValid(out _) == false)
                return false;

            if (_licenseStatus.HasSnmpMonitoring)
                return true;

            const string details = "Your current license doesn't include " +
                                   "the SNMP monitoring feature";
            GenerateLicenseLimit(LimitType.Snmp, details, addNotification: true);
            return false;
        }

        public void AssertCanDelayReplication()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (_licenseStatus.HasDelayedExternalReplication)
                return;

            const string message = "Your current license doesn't include the delayed replication feature";
            throw GenerateLicenseLimit(LimitType.DelayedExternalReplication, message, addNotification: true);
        }

        public bool CanDynamicallyDistributeNodes(out LicenseLimitException licenseLimit)
        {
            if (IsValid(out licenseLimit) == false)
                return false;

            if (_licenseStatus.HasDynamicNodesDistribution)
                return true;

            const string message = "Your current license doesn't include the dynamic database distribution feature";
            GenerateLicenseLimit(LimitType.DynamicNodeDistribution, message, addNotification: true); 
            return false;
        }

        public bool HasHighlyAvailableTasks()
        {
            return _licenseStatus.HasHighlyAvailableTasks;
        }

        public string GetLastResponsibleNodeForTask(
            IDatabaseTaskStatus databaseTaskStatus,
            DatabaseTopology databaseTopology,
            IDatabaseTask databaseTask,
            NotificationCenter.NotificationCenter notificationCenter)
        {
            if (_licenseStatus.HasHighlyAvailableTasks)
                return null;

            var lastResponsibleNode = databaseTaskStatus.NodeTag;
            if (lastResponsibleNode == null)
                return null;

            if (databaseTopology.AllNodes.Count() > 1 &&
                databaseTopology.Members.Contains(lastResponsibleNode) == false)
            {
                var taskName = databaseTask.GetTaskName();
                var message = $"Node {lastResponsibleNode} cannot execute the task: '{taskName}'";
                var alert = AlertRaised.Create(
                    null,
                    $@"You've reached your license limit ({EnumHelper.GetDescription(LimitType.HighlyAvailableTasks)})",
                    message,
                    AlertType.LicenseManager_HighlyAvailableTasks,
                    NotificationSeverity.Warning,
                    key: message,
                    details: new MessageDetails
                    {
                        Message = $"The {GetTaskType(databaseTask)} task: '{taskName}' will not be executed " +
                                  $"by node {lastResponsibleNode} (because it is {GetNodeState(databaseTopology, lastResponsibleNode)}) " +
                                  $"or by any other node because your current license " +
                                  $"doesn't include highly available tasks feature. " + Environment.NewLine +
                                  $"You can choose a different mentor node that will execute the task " +
                                  $"(current mentor node state: {GetMentorNodeState(databaseTask, databaseTopology)}). " +
                                  $"Upgrading the license will allow RavenDB to manage that automatically."
                    });

                notificationCenter.Add(alert);
            }

            return lastResponsibleNode;
        }

        private static string GetTaskType(IDatabaseTask databaseTask)
        {
            switch (databaseTask)
            {
                case PeriodicBackupConfiguration _:
                    return "Backup";
                case SubscriptionState _:
                    return "Subscription";
                case RavenEtlConfiguration _:
                    return "Raven ETL";
                case SqlEtlConfiguration _:
                    return "SQL ETL";
                case ExternalReplication _:
                    return "External Replication";
                default:
                    return string.Empty;
            }
        }

        private static string GetMentorNodeState(IDatabaseTask databaseTask, DatabaseTopology databaseTopology)
        {
            var mentorNode = databaseTask.GetMentorNode();
            return mentorNode == null ? "wasn't set" : $"'{mentorNode}' is {GetNodeState(databaseTopology, mentorNode)}";
        }

        private static string GetNodeState(DatabaseTopology databaseTopology, string nodeTag)
        {
            if (databaseTopology.Promotables.Contains(nodeTag))
                return "in promotable state";

            if (databaseTopology.Rehabs.Contains(nodeTag))
                return "in rehab state";

            return "not part of the cluster";
        }

        public void AssertCanCreateEncryptedDatabase()
        {
            if (IsValid(out var licenseLimit) == false)
                throw licenseLimit;

            if (_licenseStatus.HasEncryption)
                return;

            const string message = "Your current license doesn't include the encryption feature";
            throw GenerateLicenseLimit(LimitType.Encryption, message);
        }

        private LicenseLimitException GenerateLicenseLimit(
            LimitType limitType,
            string message,
            bool addNotification = false)
        {
            var licenseLimit = new LicenseLimitException(limitType, message);
            if (addNotification)
                LicenseLimitWarning.AddLicenseLimitNotification(_serverStore.NotificationCenter, licenseLimit);

            return licenseLimit;
        }

        private bool IsValid(out LicenseLimitException licenseLimit)
        {
            if (_licenseStatus.Type != LicenseType.Invalid)
            {
                licenseLimit = null;
                return true;
            }

            const string message = "Cannot perform operation while the license is in invalid state!";
            licenseLimit = GenerateLicenseLimit(LimitType.InvalidLicense, message);
            return false;
        }

        public async Task<LicenseSupportInfo> GetLicenseSupportInfo()
        {
            var license = _serverStore.LoadLicense();
            if (license == null)
            {
                throw new InvalidOperationException("License doesn't exist");
            }

            var leaseLicenseInfo = new LeaseLicenseInfo
            {
                License = license,
                BuildInfo = _buildInfo,
                ClusterId = _serverStore.GetClusterTopology().TopologyId,
                UtilizedCores = GetUtilizedCores()
            };

            var response = await ApiHttpClient.Instance.PostAsync("/api/v2/license/support",
                    new StringContent(JsonConvert.SerializeObject(leaseLicenseInfo), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                var message = $"Couldn't get license support info, response: {responseString}, status code: {response.StatusCode}";
                if (Logger.IsInfoEnabled)
                    Logger.Info(message);

                throw new InvalidOperationException(message);
            }

            var licenseSupportStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.Read(licenseSupportStream, "license support info");

                return JsonDeserializationServer.LicenseSupportInfo(json);
            }
        }

        public void AcceptEula()
        {
            if (_eulaAcceptedButHasPendingRestart)
                return;

            lock (_locker)
            {
                if (_eulaAcceptedButHasPendingRestart)
                    return;

                var settingsPath = _serverStore.Configuration.ConfigPath;

                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    BlittableJsonReaderObject settingsJson;

                    using (var fs = SafeFileStream.Create(settingsPath, FileMode.Open, FileAccess.Read))
                    {
                        settingsJson = context.ReadForMemory(fs, "settings-json");
                        settingsJson.Modifications = new DynamicJsonValue(settingsJson);
                        settingsJson.Modifications[RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = true;
                    }

                    var modifiedJsonObj = context.ReadObject(settingsJson, "modified-settings-json");

                    var indentedJson = SetupManager.IndentJsonString(modifiedJsonObj.ToString());
                    SetupManager.WriteSettingsJsonLocally(settingsPath, indentedJson);
                }
                _eulaAcceptedButHasPendingRestart = true;
            }
        }
    }
}

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Client;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Utils;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Replication.ReplicationBatchItem;
using Raven.Server.Utils;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.Counters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public unsafe class CountersStorage
    {
        public const int DbIdAsBase64Size = 22;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private static readonly Slice CountersTombstonesSlice;
        public static readonly Slice AllCountersEtagSlice;
        private static readonly Slice CollectionCountersEtagsSlice;
        private static readonly Slice CounterKeysSlice;

        public static readonly string CountersTombstones = "Counters.Tombstones";

        public const string DbIds = "@dbIds";
        public const string Values = "@vals";

        private long _countersCount;

        private readonly List<ByteStringContext<ByteStringMemoryCache>.InternalScope> _counterModificationMemoryScopes = new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

        private static readonly TableSchema CountersSchema = new TableSchema
        {
            TableType = (byte)TableType.Counters
        };

        private enum CountersTable
        {
            // Format of this is:
            // lower document id, record separator, prefix 
            CounterKey = 0,
            Etag = 1,         
            ChangeVector = 2,
            Data = 3,
            Collection = 4,
            TransactionMarker = 5
        }

        [StructLayout(LayoutKind.Explicit)]
        internal struct CounterValues
        {
            [FieldOffset(0)]
            public long Value;
            [FieldOffset(8)]
            public long Etag;
        }

        static CountersStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllCountersEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
                Slice.From(ctx, "CollectionCountersEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
                Slice.From(ctx, "CounterKeys", ByteStringType.Immutable, out CounterKeysSlice);
                Slice.From(ctx, CountersTombstones, ByteStringType.Immutable, out CountersTombstonesSlice);
            }
            CountersSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CountersTable.CounterKey,
                Count = 1,
                Name = CounterKeysSlice,
                IsGlobal = true,
            });

            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = AllCountersEtagSlice,
                IsGlobal = true
            });

            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = CollectionCountersEtagsSlice
            });
        }

        public CountersStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(CounterKeysSlice);

            TombstonesSchema.Create(tx, CountersTombstonesSlice, 16);
        }

        public IEnumerable<ReplicationBatchItem> GetCountersFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, 0))
            {
                yield return CreateReplicationBatchItem(context, result);
            }
        }

        public IEnumerable<CounterGroupDetail> GetCountersFrom(DocumentsOperationContext context, long etag, int skip, int take)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToCounterGroupDetail(context, result.Reader);
            }
        }

       public long GetNumberOfCountersToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            var indexDef = CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice];

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount);
        }

        public long GetNumberOfTombstonesToProcess(DocumentsOperationContext context, long afterEtag, out long totalCount)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstones);

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            var indexDef = TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice];

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount);
        }

        public static CounterGroupDetail TableValueToCounterGroupDetail(JsonOperationContext context, TableValueReader tvr)
        {
            return new CounterGroupDetail
            {
                CounterKey = TableValueToString(context, (int)CountersTable.CounterKey, ref tvr),
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref tvr),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvr),
                Values = GetData(context, ref tvr)
            };
        }

        public static (LazyStringValue DocId, string CounterName) ExtractDocIdAndCounterNameFromTombstone(JsonOperationContext context,
            LazyStringValue counterTombstoneId)
        {
            var p = counterTombstoneId.Buffer;
            var size = counterTombstoneId.Size;

            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = Encoding.UTF8.GetString(p + sizeOfDocId + 1, size - (sizeOfDocId + 2));

            return (doc, name);
        }

        private static ReplicationBatchItem CreateReplicationBatchItem(DocumentsOperationContext context, Table.TableValueHolder tvh)
        {
            var data = GetData(context, ref tvh.Reader);
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Counter,

                Id = TableValueToString(context, (int)CountersTable.CounterKey, ref tvh.Reader),
                ChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvh.Reader),
                Values = data,
                Collection = TableValueToId(context, (int)CountersTable.Collection, ref tvh.Reader),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvh.Reader)
            };
        
        }

        public string IncrementCounter(DocumentsOperationContext context, string documentId, string collection, string name, long delta, out bool exists)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice counterKey, out _))
            {
                BlittableJsonReaderObject data = null;
                exists = false;
                var value = delta;
                if (table.ReadByKey(counterKey, out var existing))
                {
                    data = GetData(context, ref existing);
                }

                var newETag = _documentsStorage.GenerateNextEtag();

                if (data != null)
                {
                    // Common case is that we modify the data IN PLACE
                    // as such, we must copy it before modification

                    data = data.Clone(context);

                    var dbIdIndex = GetDbIdIndex(context, data);

                    data.TryGet(Values, out BlittableJsonReaderObject counters);

                    if (counters.TryGet(name, out BlittableJsonReaderObject.RawBlob existingCounter))
                    {
                        IncrementExistingCounter(context, documentId, name, delta, ref exists, existingCounter, dbIdIndex, newETag, counters, value);
                    }
                    else
                    {
                        // counter doesn't exists 
                        CreateNewCounter(context, documentId, name, dbIdIndex, value, newETag, counters);
                    }

                    if (counters.Modifications != null)
                    {
                        var oldData = data;
                        data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        oldData.Dispose();
                    }
                }
                else
                {
                    // no counters at all

                    data = WriteNewCountersDocument(context, documentId, name, value, newETag);
                }

                var etag = _documentsStorage.GenerateNextEtag();
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty);

                using (Slice.From(context.Allocator, result.ChangeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(counterKey);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(cv);
                    tvb.Add(data.BasePointer, data.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    if (existing.Pointer == null)
                    {
                        table.Insert(tvb);
                    }
                    else
                    {
                        table.Update(existing.Id, tvb);
                    }
                }

                UpdateMetrics(counterKey, name, result.ChangeVector, collection);

                context.Transaction.AddAfterCommitNotification(new CounterChange
                {
                    ChangeVector = result.ChangeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = exists ? CounterChangeTypes.Increment : CounterChangeTypes.Put,
                    Value = value
                });

                return result.ChangeVector;
            }
        }

        private static int GetDbIdIndex(DocumentsOperationContext context, BlittableJsonReaderObject data)
        {
            var dbIdIndex = int.MaxValue;
            if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds))
            {
                for (dbIdIndex = 0; dbIdIndex < dbIds.Length; dbIdIndex++)
                {
                    if (dbIds[dbIdIndex].Equals(context.Environment.Base64Id) == false)
                        continue;

                    break;
                }

                if (dbIdIndex == dbIds.Length)
                {
                    dbIds.Modifications = new DynamicJsonArray {context.Environment.Base64Id};
                }
            }

            return dbIdIndex;
        }

        private void CreateNewCounter(DocumentsOperationContext context, string documentId, string name, int dbIdIndex, long value, long newETag,
            BlittableJsonReaderObject counters)
        {
            _countersCount++;

/*            using (GetCounterPartialKey(context, documentId, name, out var counterKey))
            {
                RemoveTombstoneIfExists(context, counterKey);
            }*/

            using (context.Allocator.Allocate((dbIdIndex + 1) * SizeOfCounterValues, out var newVal))
            {
                if (dbIdIndex > 0) 
                {
                    Memory.Set(newVal.Ptr, 0, dbIdIndex * SizeOfCounterValues);
                }

                var newEntry = (CounterValues*)newVal.Ptr + dbIdIndex;

                newEntry->Value = value;
                newEntry->Etag = newETag;

                counters.Modifications = new DynamicJsonValue(counters)
                {
                    [name] = new BlittableJsonReaderObject.RawBlob
                    {
                        Length = newVal.Length,
                        Ptr = newVal.Ptr
                    }
                };
            }
        }

        private void IncrementExistingCounter(DocumentsOperationContext context, string documentId, string name, long delta, ref bool exists, BlittableJsonReaderObject.RawBlob existingCounter,
            int dbIdIndex, long newETag, BlittableJsonReaderObject counters, long value)
        {
            var existingCount = existingCounter.Length / SizeOfCounterValues;

            if (dbIdIndex < existingCount)
            {
                exists = true;
                var counter = (CounterValues*)existingCounter.Ptr + dbIdIndex;
                try
                {
                    counter->Value = checked(counter->Value + delta); //inc
                    counter->Etag = newETag;
                }
                catch (OverflowException e)
                {
                    CounterOverflowException.ThrowFor(documentId, name, counter->Value, delta, e);
                }
            }
            else
            {
                // counter exists , but not with local DbId

                using (AddPartialValueToExistingCounter(context, existingCounter, dbIdIndex, value, newETag))
                {
                    counters.Modifications = new DynamicJsonValue(counters)
                    {
                        [name] = existingCounter
                    };
                }
            }
        }

        private BlittableJsonReaderObject WriteNewCountersDocument(DocumentsOperationContext context, string documentId, string name, long value, long newETag)
        {
            _countersCount++;

            BlittableJsonReaderObject data;
/*            using (GetCounterPartialKey(context, documentId, name, out var counterKey))
            {
                RemoveTombstoneIfExists(context, counterKey);
            }*/

            using (context.Allocator.Allocate(SizeOfCounterValues, out var newVal))
            {
                var newEntry = (CounterValues*)newVal.Ptr;
                newEntry->Value = value;
                newEntry->Etag = newETag;

                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();

                    builder.WritePropertyName(DbIds);

                    builder.StartWriteArray();
                    builder.WriteValue(context.Environment.Base64Id);
                    builder.WriteArrayEnd();

                    builder.WritePropertyName(Values);
                    builder.StartWriteObject();

                    builder.WritePropertyName(name);
                    builder.WriteRawBlob(newVal.Ptr, newVal.Length);

                    builder.WriteObjectEnd();

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    data = builder.CreateReader();
                }
            }

            return data;
        }

        public void PutCounters(DocumentsOperationContext context, string documentId, string collection, string changeVector,
            BlittableJsonReaderObject sourceData)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return;
            }

            try
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);
                BlittableJsonReaderObject data = null;
                BlittableJsonReaderObject sourceCounters = null;

                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice counterKey, out _))
                {
                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        if (changeVector != null && table.ReadByKey(counterKey, out var existing))
                        {
                            var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);

                            if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                                return;

                            data = GetData(context, ref existing);
                            data = data.Clone(context);
                            data.TryGet(DbIds, out BlittableJsonReaderArray dbIds);
                            data.TryGet(Values, out BlittableJsonReaderObject localCounters);

                            var localDbIdsList = DbIdsToList(dbIds);

                            sourceData.TryGet(DbIds, out BlittableJsonReaderArray sourceDbIds);
                            sourceData.TryGet(Values, out sourceCounters);

                            var prop = new BlittableJsonReaderObject.PropertyDetails();
                            for (var i = 0; i< sourceCounters.Count; i++)
                            {
                                sourceCounters.GetPropertyByIndex(i, ref prop);

                                var counterName = prop.Name;

                                var source = (BlittableJsonReaderObject.RawBlob)prop.Value;

                                long value = 0;
                                bool modified;
                                var changeType = CounterChangeTypes.Put;

                                if (source.Length == 0)
                                {
                                    // Delete
                                    modified = true;
                                    changeType = CounterChangeTypes.Delete;
                                    localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                                    localCounters.Modifications[counterName] = new BlittableJsonReaderObject.RawBlob();
                                    _countersCount--;
                                }

                                else
                                {
                                    if (localCounters.TryGet(counterName, out BlittableJsonReaderObject.RawBlob existingCounter) == false)
                                    {
                                        _countersCount++;
                                        existingCounter = new BlittableJsonReaderObject.RawBlob();
                                    }

                                    value = InternalPutCounter(context, localCounters, counterName, dbIds, sourceDbIds, localDbIdsList, existingCounter, source, out modified);
                                }

                                if (modified == false)
                                    continue;

                                context.Transaction.AddAfterCommitNotification(new CounterChange
                                {
                                    ChangeVector = changeVector,
                                    DocumentId = documentId,
                                    Name = counterName,
                                    Value = value,
                                    Type = changeType
                                });

                                UpdateMetrics(counterKey, counterName, changeVector, collection);
                            }

                            if (localCounters.Modifications != null)
                            {
                                var oldData = data;

                                data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                                oldData.Dispose();
                            }
                        }

                        if (data == null)
                        {
                            data = context.ReadObject(sourceData, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            _countersCount += sourceCounters?.Count ?? 0;
                        }

                        var etag = _documentsStorage.GenerateNextEtag();

                        if (changeVector == null)
                        {
                            changeVector = ChangeVectorUtils
                                .TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty)
                                .ChangeVector;
                        }

                        using (Slice.From(context.Allocator, changeVector, out var cv))
                        using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                        {
                            tvb.Add(counterKey);
                            tvb.Add(Bits.SwapBytes(etag));
                            tvb.Add(cv);
                            tvb.Add(data.BasePointer, data.Size);
                            tvb.Add(collectionSlice);
                            tvb.Add(context.TransactionMarkerOffset);

                            table.Set(tvb);
                        }
                    }
                }
            }
            finally 
            {
                foreach (var s in _counterModificationMemoryScopes)
                {
                    s.Dispose();
                }
                _counterModificationMemoryScopes.Clear();              
            }
        }

        private static List<LazyStringValue> DbIdsToList(BlittableJsonReaderArray dbIds)
        {
            var localDbIdsList = new List<LazyStringValue>(dbIds.Length);
            for (int i = 0; i < dbIds.Length; i++)
            {
                localDbIdsList.Add((LazyStringValue)dbIds[i]);
            }

            return localDbIdsList;
        }

        private long InternalPutCounter(DocumentsOperationContext context, BlittableJsonReaderObject counters, string counterName,
            BlittableJsonReaderArray localDbIds, BlittableJsonReaderArray sourceDbIds, List<LazyStringValue> localDbIdsList, 
            BlittableJsonReaderObject.RawBlob existingCounter, BlittableJsonReaderObject.RawBlob source, out bool modified)
        {
            long value = 0;
            var existingCount = existingCounter.Length / SizeOfCounterValues;
            var sourceCount = source.Length / SizeOfCounterValues;
            modified = false;

            for (var index = 0; index < sourceCount; index++)
            {
                var sourceDbId = (LazyStringValue)sourceDbIds[index];
                var sourceValue = ((CounterValues*)source.Ptr)[index];

                int localDbIdIndex = GetOrAddDbIdIndex(localDbIds, localDbIdsList, sourceDbId);

                if (localDbIdIndex < existingCount)
                {
                    var localValuePtr = (CounterValues*)existingCounter.Ptr + localDbIdIndex;
                    if (localValuePtr->Etag >= sourceValue.Etag ||
                        sourceDbId.Equals(context.Environment.Base64Id))
                    {
                        value += localValuePtr->Value;
                        continue;
                    }

                    localValuePtr->Value = sourceValue.Value;
                    localValuePtr->Etag = sourceValue.Etag;

                    value += sourceValue.Value;
                    continue;
                }

                // counter doesn't have this dbId
                modified = true;
                value += sourceValue.Value;
                var scope = AddPartialValueToExistingCounter(context, existingCounter, localDbIdIndex, sourceValue.Value, sourceValue.Etag);
                _counterModificationMemoryScopes.Add(scope);

                existingCount = existingCounter.Length / SizeOfCounterValues;
            }

            if (modified)
            {
                counters.Modifications = counters.Modifications ?? new DynamicJsonValue(counters);
                counters.Modifications[counterName] = existingCounter;
            }

            return value;
        }

        private static int GetOrAddDbIdIndex(BlittableJsonReaderArray localDbIds, List<LazyStringValue> localDbIdsList, LazyStringValue dbId)
        {
            int dbIdIndex;
            for (dbIdIndex = 0; dbIdIndex < localDbIdsList.Count; dbIdIndex++)
            {
                var current = localDbIdsList[dbIdIndex];
                if (current.Equals(dbId) == false)
                    continue;
                break;
            }

            if (dbIdIndex == localDbIdsList.Count)
            {
                localDbIdsList.Add(dbId);
                localDbIds.Modifications = localDbIds.Modifications ?? new DynamicJsonArray();
                localDbIds.Modifications.Add(dbId);
            }

            return dbIdIndex;
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope AddPartialValueToExistingCounter(DocumentsOperationContext context, 
            BlittableJsonReaderObject.RawBlob existingCounter, int dbIdIndex, long sourceValue, long sourceEtag)
        {
            var scope = context.Allocator.Allocate((dbIdIndex + 1) * SizeOfCounterValues, out var newVal);

            Memory.Copy(newVal.Ptr, existingCounter.Ptr, existingCounter.Length);
            var empties = dbIdIndex - existingCounter.Length / SizeOfCounterValues;
            if (empties > 0)
            {
                Memory.Set(newVal.Ptr + existingCounter.Length, 0, empties * SizeOfCounterValues);
            }
;
            var newEntry = (CounterValues*)newVal.Ptr + dbIdIndex;
            newEntry->Value = sourceValue;
            newEntry->Etag = sourceEtag;

            existingCounter.Ptr = newVal.Ptr;
            existingCounter.Length = newVal.Length;

            return scope;

        }

        private void UpdateMetrics(Slice counterKey, string counterName, string changeVector, string collection)
        {
            _documentDatabase.Metrics.Counters.PutsPerSec.MarkSingleThreaded(1);
            var bytesPutsInBytes =
                counterKey.Size + counterName.Length
                                + sizeof(long) // etag 
                                + sizeof(long) // counter value
                                + changeVector.Length + collection.Length;

            _documentDatabase.Metrics.Counters.BytesPutsPerSec.MarkSingleThreaded(bytesPutsInBytes);
        }

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public static int SizeOfCounterValues = sizeof(CounterValues);

        public Table GetCountersTable(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.Counters);

            if (tx.IsWriteTransaction && _tableCreated.Contains(collection.Name) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                CountersSchema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case 
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                     {
                         collection.Name
                     };
                };
            }

            return tx.OpenTable(CountersSchema, tableName);
        }

        public IEnumerable<string> GetCountersForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var existing) == false)
                    yield break;

                var data = GetData(context, ref existing);
                data.TryGet(Values, out BlittableJsonReaderObject counters);

                var prop = new BlittableJsonReaderObject.PropertyDetails();
                for (var i=0; i < counters.Count; i++)
                {
                    counters.GetPropertyByIndex(i, ref prop);
                    var blob = (BlittableJsonReaderObject.RawBlob)prop.Value;
                    if (blob.Length == 0)
                        continue;

                    yield return prop.Name;
                }
            }
        }

        private static BlittableJsonReaderObject GetData(JsonOperationContext context, ref TableValueReader existing)
        {
            return new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
        }

        public long? GetCounterValue(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var tvr) == false)
                    return null;

                var data = GetData(context, ref tvr);
                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGet(counterName, out BlittableJsonReaderObject.RawBlob counterValues) == false ||
                    counterValues.Length == 0)
                    return null;

                var existingCount = counterValues.Length / SizeOfCounterValues;

                long value = 0;
                for (var i= 0; i < existingCount; i++) 
                {
                    value += GetPartialValue(i, counterValues);
                }

                return value;
            }
        }

        public IEnumerable<(string ChangeVector, long Value)> GetCounterValues(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var tvr) == false)
                    yield break;

                var data = GetData(context, ref tvr);
                if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false ||
                    data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGet(counterName, out BlittableJsonReaderObject.RawBlob counterValues) == false ||
                    counterValues.Length == 0)                  
                    yield break;

                var existingCount = counterValues.Length / SizeOfCounterValues;

                for (var dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex ++)
                {
                    var val = GetPartialValue(dbIdIndex, counterValues);
                    yield return (dbIds[dbIdIndex].ToString(), val);
                }
            }
        }

        private static long GetPartialValue(int index, BlittableJsonReaderObject.RawBlob counterValues)
        {
            return ((CounterValues*)counterValues.Ptr)[index].Value;
        }

        public ByteStringContext.InternalScope GetCounterPartialKey(DocumentsOperationContext context, string documentId, string name, out Slice partialKeySlice)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out var nameLower, out _))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       + nameLower.Size
                                                       + 1 // record separator
                                                       , out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;

                byte* dest = buffer.Ptr + docIdLower.Size + 1;
                nameLower.CopyTo(dest);
                dest[nameLower.Size] = SpecialChars.RecordSeparator;

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        public void DeleteCountersForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will called as part of document's delete, so we don't bother creating
            // tombstones (existing tombstones will remain and be cleaned up by the usual
            // tombstone cleaner task

            var table = GetCountersTable(context.Transaction.InnerTransaction, collection);

            if (table.NumberOfEntries == 0)
                return;

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                //todo update Count of Counters
                table.DeleteByPrimaryKeyPrefix(lowerId /*, holder => _countersCount -= GetData(context, ref holder.Reader).Count -1*/); 
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, Slice counterKey, string collection, long lastModifiedTicks, bool forceTombstone)
        {
            var (doc, name) = ExtractDocIdAndName(context, counterKey);
            return DeleteCounter(context, doc, collection, name, forceTombstone, lastModifiedTicks);
        }

        public string DeleteCounter(DocumentsOperationContext context, string documentId, string collection, string counterName, bool forceTombstone = false, long lastModifiedTicks = -1)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                if (table.ReadByKey(lowerId, out var existing) == false)               
                    return null;
                
                var data = GetData(context, ref existing);

                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGet(counterName, out BlittableJsonReaderObject.RawBlob counterToDelete) == false ||
                    counterToDelete.Length == 0)                
                    return null;
                
                RemoveCounterFromBlittableAndUpdateTable(context, counterName, data, counters, collectionName, table, lowerId, out var cv);

                _countersCount--;

                return cv;
            }
        }

        private void RemoveCounterFromBlittableAndUpdateTable(DocumentsOperationContext context, string counterName, BlittableJsonReaderObject data,
            BlittableJsonReaderObject counters, CollectionName collectionName, Table table,
            Slice lowerId, out string newChangeVector)
        {
            var oldData = data;

            counters.Modifications = new DynamicJsonValue(counters)
            {
                [counterName] = new BlittableJsonReaderObject.RawBlob()
            };

            data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            oldData.Dispose();

            var newEtag = _documentsStorage.GenerateNextEtag();
            newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);
            using (Slice.From(context.Allocator, newChangeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(lowerId);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(cv);
                tvb.Add(data.BasePointer, data.Size);
                tvb.Add(collectionSlice);
                tvb.Add(context.TransactionMarkerOffset);

                table.Set(tvb);
            }
        }

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndName(JsonOperationContext context, Slice counterKey)
        {
            var p = counterKey.Content.Ptr;
            var size = counterKey.Size;
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);

            sizeOfDocId++;
            p += sizeOfDocId;
            int sizeOfName = size - sizeOfDocId - 1;
            var name = context.AllocateStringValue(null, p, sizeOfName);
            return (doc, name);
        }

        public static void AssertCounters(BlittableJsonReaderObject document, DocumentFlags flags)
        {
            if ((flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray _) == false)
                {
                    Debug.Assert(false, $"Found {DocumentFlags.HasCounters} flag but {Constants.Documents.Metadata.Counters} is missing from metadata.");
                }
            }
            else
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters))
                {
                    Debug.Assert(false, $"Found {Constants.Documents.Metadata.Counters}({counters.Length}) in metadata but {DocumentFlags.HasCounters} flag is missing.");
                }
            }
        }

        public long GetNumberOfCounterEntries(DocumentsOperationContext context)
        {
            return _countersCount;
        }

        public void UpdateDocumentCounters(DocumentsOperationContext context, Document doc, string docId,
            SortedSet<string> countersToAdd, HashSet<string> countersToRemove, NonPersistentDocumentFlags nonPersistentDocumentFlags)
        {
            if (countersToRemove.Count == 0 && countersToAdd.Count == 0)
                return;
            var data = doc.Data;
            BlittableJsonReaderArray metadataCounters = null;
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.Counters, out metadataCounters);
            }

            var counters = GetCountersForDocument(metadataCounters, countersToAdd, countersToRemove, out var hadModifications);
            if (hadModifications == false)
                return;

            var flags = doc.Flags.Strip(DocumentFlags.HasCounters);
            if (counters.Count == 0)
            {
                if (metadata != null)
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    data.Modifications = new DynamicJsonValue(data)
                    {
                        [Constants.Documents.Metadata.Key] = metadata
                    };
                }
            }
            else
            {
                flags |= DocumentFlags.HasCounters;
                data.Modifications = new DynamicJsonValue(data);
                if (metadata == null)
                {
                    data.Modifications[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(counters)
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(counters)
                    };
                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
            }

            var newDocumentData = context.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            _documentDatabase.DocumentsStorage.Put(context, docId, null, newDocumentData, flags: flags, nonPersistentFlags: nonPersistentDocumentFlags);
        }

        private static SortedSet<string> GetCountersForDocument(BlittableJsonReaderArray metadataCounters, SortedSet<string> countersToAdd, HashSet<string> countersToRemove, out bool modified)
        {
            modified = false;
            if (metadataCounters == null)
            {
                modified = true;
                return countersToAdd;
            }

            foreach (var counter in metadataCounters)
            {
                var str = counter.ToString();
                if (countersToRemove.Contains(str))
                {
                    modified = true;
                    continue;
                }

                countersToAdd.Add(str);
            }

            if (modified == false)
            {
                // if no counter was removed, we can be sure that there are no modification when the counter's count in the metadata is equal to the count of countersToAdd 
                modified = countersToAdd.Count != metadataCounters.Length;
            }

            return countersToAdd;
        }

        public static void ConvertFromBlobToNumbers(JsonOperationContext context, CounterGroupDetail counterGroupDetail)
        {
            counterGroupDetail.Values.TryGet(Values, out BlittableJsonReaderObject counters);
            counters.Modifications = new DynamicJsonValue(counters);

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < counters.Count; i++)
            {
                counters.GetPropertyByIndex(i, ref prop);

                var dja = new DynamicJsonArray();
                var blob = (BlittableJsonReaderObject.RawBlob)prop.Value;
                var existingCount = blob.Length / SizeOfCounterValues;

                for (int dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex++)
                {
                    var current = (CounterValues*)blob.Ptr + dbIdIndex;

                    dja.Add(current->Value);
                    dja.Add(current->Etag);
                }

                counters.Modifications[prop.Name] = dja;
            }

            var oldData = counterGroupDetail.Values;

            counterGroupDetail.Values = context.ReadObject(counterGroupDetail.Values, null);

            oldData.Dispose();
        }

    }
}

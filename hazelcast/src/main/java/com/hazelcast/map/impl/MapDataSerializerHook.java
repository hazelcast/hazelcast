/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl;

import com.hazelcast.client.impl.protocol.task.map.MapAssignAndGetUuidsOperation;
import com.hazelcast.client.impl.protocol.task.map.MapAssignAndGetUuidsOperationFactory;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.journal.DeserializingEventJournalMapEvent;
import com.hazelcast.map.impl.journal.InternalEventJournalMapEvent;
import com.hazelcast.map.impl.journal.MapEventJournalReadOperation;
import com.hazelcast.map.impl.journal.MapEventJournalReadResultSetImpl;
import com.hazelcast.map.impl.journal.MapEventJournalSubscribeOperation;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.map.impl.operation.AccumulatorConsumerOperation;
import com.hazelcast.map.impl.operation.AddIndexOperation;
import com.hazelcast.map.impl.operation.AddIndexOperationFactory;
import com.hazelcast.map.impl.operation.AddInterceptorOperation;
import com.hazelcast.map.impl.operation.AwaitMapFlushOperation;
import com.hazelcast.map.impl.operation.ClearBackupOperation;
import com.hazelcast.map.impl.operation.ClearNearCacheOperation;
import com.hazelcast.map.impl.operation.ClearOperation;
import com.hazelcast.map.impl.operation.ClearOperationFactory;
import com.hazelcast.map.impl.operation.ContainsKeyOperation;
import com.hazelcast.map.impl.operation.ContainsValueOperation;
import com.hazelcast.map.impl.operation.ContainsValueOperationFactory;
import com.hazelcast.map.impl.operation.DeleteOperation;
import com.hazelcast.map.impl.operation.EntryBackupOperation;
import com.hazelcast.map.impl.operation.EntryOffloadableSetUnlockOperation;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.map.impl.operation.EvictAllBackupOperation;
import com.hazelcast.map.impl.operation.EvictAllOperation;
import com.hazelcast.map.impl.operation.EvictAllOperationFactory;
import com.hazelcast.map.impl.operation.EvictBackupOperation;
import com.hazelcast.map.impl.operation.EvictBatchBackupOperation;
import com.hazelcast.map.impl.operation.EvictOperation;
import com.hazelcast.map.impl.operation.GetAllOperation;
import com.hazelcast.map.impl.operation.GetEntryViewOperation;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.IsEmptyOperationFactory;
import com.hazelcast.map.impl.operation.IsKeyLoadFinishedOperation;
import com.hazelcast.map.impl.operation.IsPartitionLoadedOperation;
import com.hazelcast.map.impl.operation.IsPartitionLoadedOperationFactory;
import com.hazelcast.map.impl.operation.KeyLoadStatusOperation;
import com.hazelcast.map.impl.operation.KeyLoadStatusOperationFactory;
import com.hazelcast.map.impl.operation.LegacyMergeOperation;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.map.impl.operation.LoadMapOperation;
import com.hazelcast.map.impl.operation.MapFetchEntriesOperation;
import com.hazelcast.map.impl.operation.MapFetchKeysOperation;
import com.hazelcast.map.impl.operation.MapFetchWithQueryOperation;
import com.hazelcast.map.impl.operation.MapFlushBackupOperation;
import com.hazelcast.map.impl.operation.MapFlushOperation;
import com.hazelcast.map.impl.operation.MapFlushOperationFactory;
import com.hazelcast.map.impl.operation.MapGetAllOperationFactory;
import com.hazelcast.map.impl.operation.MapGetInvalidationMetaDataOperation;
import com.hazelcast.map.impl.operation.MapIsEmptyOperation;
import com.hazelcast.map.impl.operation.MapLoadAllOperationFactory;
import com.hazelcast.map.impl.operation.MapNearCacheStateHolder;
import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.operation.MapReplicationStateHolder;
import com.hazelcast.map.impl.operation.MapSizeOperation;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.operation.MergeOperationFactory;
import com.hazelcast.map.impl.operation.MultipleEntryBackupOperation;
import com.hazelcast.map.impl.operation.MultipleEntryOperation;
import com.hazelcast.map.impl.operation.MultipleEntryOperationFactory;
import com.hazelcast.map.impl.operation.MultipleEntryWithPredicateBackupOperation;
import com.hazelcast.map.impl.operation.MultipleEntryWithPredicateOperation;
import com.hazelcast.map.impl.operation.NotifyMapFlushOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryBackupOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryOperationFactory;
import com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateBackupOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateOperationFactory;
import com.hazelcast.map.impl.operation.PostJoinMapOperation;
import com.hazelcast.map.impl.operation.PutAllBackupOperation;
import com.hazelcast.map.impl.operation.PutAllOperation;
import com.hazelcast.map.impl.operation.PutAllPartitionAwareOperationFactory;
import com.hazelcast.map.impl.operation.PutBackupOperation;
import com.hazelcast.map.impl.operation.PutFromLoadAllBackupOperation;
import com.hazelcast.map.impl.operation.PutFromLoadAllOperation;
import com.hazelcast.map.impl.operation.PutIfAbsentOperation;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.map.impl.operation.PutTransientOperation;
import com.hazelcast.map.impl.operation.RemoveBackupOperation;
import com.hazelcast.map.impl.operation.RemoveFromLoadAllOperation;
import com.hazelcast.map.impl.operation.RemoveIfSameOperation;
import com.hazelcast.map.impl.operation.RemoveInterceptorOperation;
import com.hazelcast.map.impl.operation.RemoveOperation;
import com.hazelcast.map.impl.operation.ReplaceIfSameOperation;
import com.hazelcast.map.impl.operation.ReplaceOperation;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.map.impl.operation.SetTtlBackupOperation;
import com.hazelcast.map.impl.operation.SetTtlOperation;
import com.hazelcast.map.impl.operation.SizeOperationFactory;
import com.hazelcast.map.impl.operation.TriggerLoadIfNeededOperation;
import com.hazelcast.map.impl.operation.TryPutOperation;
import com.hazelcast.map.impl.operation.TryRemoveOperation;
import com.hazelcast.map.impl.operation.WriteBehindStateHolder;
import com.hazelcast.map.impl.query.AggregationResult;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.map.impl.query.Target;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperationFactory;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.ReadAndResetAccumulatorOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.tx.MapTransactionLogRecord;
import com.hazelcast.map.impl.tx.TxnDeleteOperation;
import com.hazelcast.map.impl.tx.TxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.TxnPrepareBackupOperation;
import com.hazelcast.map.impl.tx.TxnPrepareOperation;
import com.hazelcast.map.impl.tx.TxnRollbackBackupOperation;
import com.hazelcast.map.impl.tx.TxnRollbackOperation;
import com.hazelcast.map.impl.tx.TxnSetOperation;
import com.hazelcast.map.impl.tx.TxnUnlockBackupOperation;
import com.hazelcast.map.impl.tx.TxnUnlockOperation;
import com.hazelcast.map.impl.tx.VersionedValue;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_DS_FACTORY_ID;

public final class MapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MAP_DS_FACTORY, MAP_DS_FACTORY_ID);

    public static final int PUT = 0;
    public static final int GET = 1;
    public static final int REMOVE = 2;
    public static final int PUT_BACKUP = 3;
    public static final int REMOVE_BACKUP = 4;
    public static final int KEY_SET = 5;
    public static final int VALUES = 6;
    public static final int ENTRIES = 7;
    public static final int ENTRY_VIEW = 8;
    public static final int QUERY_RESULT_ROW = 9;
    public static final int QUERY_RESULT = 10;
    public static final int EVICT_BACKUP = 11;
    public static final int CONTAINS_KEY = 12;
    public static final int KEYS_WITH_CURSOR = 13;
    public static final int ENTRIES_WITH_CURSOR = 14;
    public static final int SET = 15;
    public static final int LOAD_MAP = 16;
    public static final int KEY_LOAD_STATUS = 17;
    public static final int LOAD_ALL = 18;
    public static final int ENTRY_BACKUP = 19;
    public static final int ENTRY_OPERATION = 20;
    public static final int PUT_ALL = 21;
    public static final int PUT_ALL_BACKUP = 22;
    public static final int REMOVE_IF_SAME = 23;
    public static final int REPLACE = 24;
    public static final int SIZE = 25;
    public static final int CLEAR_BACKUP = 26;
    public static final int CLEAR_NEAR_CACHE = 27;
    public static final int CLEAR = 28;
    public static final int DELETE = 29;
    public static final int EVICT = 30;
    public static final int EVICT_ALL = 31;
    public static final int EVICT_ALL_BACKUP = 32;
    public static final int GET_ALL = 33;
    public static final int IS_EMPTY = 34;
    public static final int LEGACY_MERGE = 35;
    public static final int NEAR_CACHE_SINGLE_INVALIDATION = 36;
    public static final int NEAR_CACHE_BATCH_INVALIDATION = 37;
    public static final int IS_PARTITION_LOADED = 38;
    public static final int PARTITION_WIDE_ENTRY = 39;
    public static final int PARTITION_WIDE_ENTRY_BACKUP = 40;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY = 41;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_BACKUP = 42;
    public static final int ADD_INDEX = 43;
    public static final int AWAIT_MAP_FLUSH = 44;
    public static final int CONTAINS_VALUE = 45;
    public static final int GET_ENTRY_VIEW = 46;
    public static final int FETCH_ENTRIES = 47;
    public static final int FETCH_KEYS = 48;
    public static final int FLUSH_BACKUP = 49;
    public static final int FLUSH = 50;
    public static final int MULTIPLE_ENTRY_BACKUP = 51;
    public static final int MULTIPLE_ENTRY = 52;
    public static final int MULTIPLE_ENTRY_PREDICATE_BACKUP = 53;
    public static final int MULTIPLE_ENTRY_PREDICATE = 54;
    public static final int NOTIFY_MAP_FLUSH = 55;
    public static final int PUT_IF_ABSENT = 56;
    public static final int PUT_FROM_LOAD_ALL = 57;
    public static final int PUT_FROM_LOAD_ALL_BACKUP = 58;
    public static final int QUERY_PARTITION = 59;
    public static final int QUERY_OPERATION = 60;
    public static final int PUT_TRANSIENT = 61;
    public static final int REPLACE_IF_SAME = 62;
    public static final int TRY_PUT = 63;
    public static final int TRY_REMOVE = 64;
    public static final int TXN_LOCK_AND_GET = 65;
    public static final int TXN_DELETE = 66;
    public static final int TXN_PREPARE = 67;
    public static final int TXN_PREPARE_BACKUP = 68;
    public static final int TXN_ROLLBACK = 69;
    public static final int TXN_ROLLBACK_BACKUP = 70;
    public static final int TXN_SET = 71;
    public static final int TXN_UNLOCK = 72;
    public static final int TXN_UNLOCK_BACKUP = 73;
    public static final int IS_PARTITION_LOADED_FACTORY = 74;
    public static final int ADD_INDEX_FACTORY = 75;
    public static final int ADD_INTERCEPTOR_FACTORY = 76;
    public static final int CLEAR_FACTORY = 77;
    public static final int CONTAINS_VALUE_FACTORY = 78;
    public static final int EVICT_ALL_FACTORY = 79;
    public static final int IS_EMPTY_FACTORY = 80;
    public static final int KEY_LOAD_STATUS_FACTORY = 81;
    public static final int MAP_FLUSH_FACTORY = 82;
    public static final int MAP_GET_ALL_FACTORY = 83;
    public static final int LOAD_ALL_FACTORY = 84;
    public static final int PARTITION_WIDE_ENTRY_FACTORY = 85;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_FACTORY = 86;
    public static final int PUT_ALL_PARTITION_AWARE_FACTORY = 87;
    public static final int REMOVE_INTERCEPTOR_FACTORY = 88;
    public static final int SIZE_FACTORY = 89;
    public static final int MULTIPLE_ENTRY_FACTORY = 90;
    public static final int ENTRY_EVENT_FILTER = 91;
    public static final int EVENT_LISTENER_FILTER = 92;
    public static final int PARTITION_LOST_EVENT_FILTER = 93;
    public static final int NEAR_CACHE_CLEAR_INVALIDATION = 94;
    public static final int ADD_INTERCEPTOR = 95;
    public static final int MAP_REPLICATION = 96;
    public static final int POST_JOIN_MAP_OPERATION = 97;
    public static final int INDEX_INFO = 98;
    public static final int MAP_INDEX_INFO = 99;
    public static final int INTERCEPTOR_INFO = 100;
    public static final int REMOVE_INTERCEPTOR = 101;
    public static final int QUERY_EVENT_FILTER = 102;
    public static final int RECORD_INFO = 103;
    public static final int RECORD_REPLICATION_INFO = 104;
    public static final int HIGHER_HITS_MERGE_POLICY = 105;
    public static final int LATEST_UPDATE_MERGE_POLICY = 106;
    public static final int PASS_THROUGH_MERGE_POLICY = 107;
    public static final int PUT_IF_ABSENT_MERGE_POLICY = 108;
    public static final int UUID_FILTER = 109;
    public static final int CLEAR_NEAR_CACHE_INVALIDATION = 110;
    public static final int MAP_TRANSACTION_LOG_RECORD = 111;
    public static final int VERSIONED_VALUE = 112;
    public static final int MAP_REPLICATION_STATE_HOLDER = 113;
    public static final int WRITE_BEHIND_STATE_HOLDER = 114;
    public static final int AGGREGATION_RESULT = 115;
    public static final int QUERY = 116;
    public static final int TARGET = 117;
    public static final int MAP_INVALIDATION_METADATA = 118;
    public static final int MAP_INVALIDATION_METADATA_RESPONSE = 119;
    public static final int MAP_NEAR_CACHE_STATE_HOLDER = 120;
    public static final int MAP_ASSIGN_AND_GET_UUIDS = 121;
    public static final int MAP_ASSIGN_AND_GET_UUIDS_FACTORY = 122;
    public static final int DESTROY_QUERY_CACHE = 123;
    public static final int MADE_PUBLISHABLE = 124;
    public static final int MADE_PUBLISHABLE_FACTORY = 125;
    public static final int PUBLISHER_CREATE = 126;
    public static final int READ_AND_RESET_ACCUMULATOR = 127;
    public static final int SET_READ_CURSOR = 128;
    public static final int ACCUMULATOR_CONSUMER = 129;
    public static final int LAZY_MAP_ENTRY = 131;
    public static final int TRIGGER_LOAD_IF_NEEDED = 132;
    public static final int IS_KEYLOAD_FINISHED = 133;
    public static final int REMOVE_FROM_LOAD_ALL = 134;
    public static final int ENTRY_REMOVING_PROCESSOR = 135;
    public static final int ENTRY_OFFLOADABLE_SET_UNLOCK = 136;
    public static final int LOCK_AWARE_LAZY_MAP_ENTRY = 137;
    public static final int FETCH_WITH_QUERY = 138;
    public static final int RESULT_SEGMENT = 139;
    public static final int EVICT_BATCH_BACKUP = 140;
    public static final int EVENT_JOURNAL_SUBSCRIBE_OPERATION = 141;
    public static final int EVENT_JOURNAL_READ = 142;
    public static final int EVENT_JOURNAL_DESERIALIZING_MAP_EVENT = 143;
    public static final int EVENT_JOURNAL_INTERNAL_MAP_EVENT = 144;
    public static final int EVENT_JOURNAL_READ_RESULT_SET = 145;
    public static final int MERGE_FACTORY = 146;
    public static final int MERGE = 147;
    public static final int SET_TTL = 148;
    public static final int SET_TTL_BACKUP = 149;
    public static final int MERKLE_TREE_NODE_ENTRIES = 150;

    private static final int LEN = MERKLE_TREE_NODE_ENTRIES + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[PUT] = arg -> new PutOperation();
        constructors[GET] = arg -> new GetOperation();
        constructors[REMOVE] = arg -> new RemoveOperation();
        constructors[PUT_BACKUP] = arg -> new PutBackupOperation();
        constructors[REMOVE_BACKUP] = arg -> new RemoveBackupOperation();
        constructors[EVICT_BACKUP] = arg -> new EvictBackupOperation();
        constructors[KEY_SET] = arg -> new MapKeySet();
        constructors[VALUES] = arg -> new MapValueCollection();
        constructors[ENTRIES] = arg -> new MapEntries();
        constructors[ENTRY_VIEW] = arg -> (IdentifiedDataSerializable) EntryViews.createSimpleEntryView();
        constructors[QUERY_RESULT_ROW] = arg -> new QueryResultRow();
        constructors[QUERY_RESULT] = arg -> new QueryResult();
        constructors[CONTAINS_KEY] = arg -> new ContainsKeyOperation();
        constructors[KEYS_WITH_CURSOR] = arg -> new MapKeysWithCursor();
        constructors[ENTRIES_WITH_CURSOR] = arg -> new MapEntriesWithCursor();
        constructors[SET] = arg -> new SetOperation();
        constructors[LOAD_MAP] = arg -> new LoadMapOperation();
        constructors[KEY_LOAD_STATUS] = arg -> new KeyLoadStatusOperation();
        constructors[LOAD_ALL] = arg -> new LoadAllOperation();
        constructors[ENTRY_BACKUP] = arg -> new EntryBackupOperation();
        constructors[ENTRY_OPERATION] = arg -> new EntryOperation();
        constructors[PUT_ALL] = arg -> new PutAllOperation();
        constructors[PUT_ALL_BACKUP] = arg -> new PutAllBackupOperation();
        constructors[REMOVE_IF_SAME] = arg -> new RemoveIfSameOperation();
        constructors[REPLACE] = arg -> new ReplaceOperation();
        constructors[SIZE] = arg -> new MapSizeOperation();
        constructors[CLEAR_BACKUP] = arg -> new ClearBackupOperation();
        constructors[CLEAR_NEAR_CACHE] = arg -> new ClearNearCacheOperation();
        constructors[CLEAR] = arg -> new ClearOperation();
        constructors[DELETE] = arg -> new DeleteOperation();
        constructors[EVICT] = arg -> new EvictOperation();
        constructors[EVICT_ALL] = arg -> new EvictAllOperation();
        constructors[EVICT_ALL_BACKUP] = arg -> new EvictAllBackupOperation();
        constructors[GET_ALL] = arg -> new GetAllOperation();
        constructors[IS_EMPTY] = arg -> new MapIsEmptyOperation();
        constructors[LEGACY_MERGE] = arg -> new LegacyMergeOperation();
        constructors[IS_PARTITION_LOADED] = arg -> new IsPartitionLoadedOperation();
        constructors[PARTITION_WIDE_ENTRY] = arg -> new PartitionWideEntryOperation();
        constructors[PARTITION_WIDE_ENTRY_BACKUP] = arg -> new PartitionWideEntryBackupOperation();
        constructors[PARTITION_WIDE_PREDICATE_ENTRY] = arg -> new PartitionWideEntryWithPredicateOperation();
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_BACKUP] = arg -> new PartitionWideEntryWithPredicateBackupOperation();
        constructors[ADD_INDEX] = arg -> new AddIndexOperation();
        constructors[AWAIT_MAP_FLUSH] = arg -> new AwaitMapFlushOperation();
        constructors[CONTAINS_VALUE] = arg -> new ContainsValueOperation();
        constructors[GET_ENTRY_VIEW] = arg -> new GetEntryViewOperation();
        constructors[FETCH_ENTRIES] = arg -> new MapFetchEntriesOperation();
        constructors[FETCH_KEYS] = arg -> new MapFetchKeysOperation();
        constructors[FLUSH_BACKUP] = arg -> new MapFlushBackupOperation();
        constructors[FLUSH] = arg -> new MapFlushOperation();
        constructors[MULTIPLE_ENTRY_BACKUP] = arg -> new MultipleEntryBackupOperation();
        constructors[MULTIPLE_ENTRY] = arg -> new MultipleEntryOperation();
        constructors[MULTIPLE_ENTRY_PREDICATE_BACKUP] = arg -> new MultipleEntryWithPredicateBackupOperation();
        constructors[MULTIPLE_ENTRY_PREDICATE] = arg -> new MultipleEntryWithPredicateOperation();
        constructors[NOTIFY_MAP_FLUSH] = arg -> new NotifyMapFlushOperation();
        constructors[PUT_IF_ABSENT] = arg -> new PutIfAbsentOperation();
        constructors[PUT_FROM_LOAD_ALL] = arg -> new PutFromLoadAllOperation();
        constructors[PUT_FROM_LOAD_ALL_BACKUP] = arg -> new PutFromLoadAllBackupOperation();
        constructors[QUERY_PARTITION] = arg -> new QueryPartitionOperation();
        constructors[QUERY_OPERATION] = arg -> new QueryOperation();
        constructors[PUT_TRANSIENT] = arg -> new PutTransientOperation();
        constructors[REPLACE_IF_SAME] = arg -> new ReplaceIfSameOperation();
        constructors[TRY_PUT] = arg -> new TryPutOperation();
        constructors[TRY_REMOVE] = arg -> new TryRemoveOperation();
        constructors[TXN_LOCK_AND_GET] = arg -> new TxnLockAndGetOperation();
        constructors[TXN_DELETE] = arg -> new TxnDeleteOperation();
        constructors[TXN_PREPARE] = arg -> new TxnPrepareOperation();
        constructors[TXN_PREPARE_BACKUP] = arg -> new TxnPrepareBackupOperation();
        constructors[TXN_ROLLBACK] = arg -> new TxnRollbackOperation();
        constructors[TXN_ROLLBACK_BACKUP] = arg -> new TxnRollbackBackupOperation();
        constructors[TXN_SET] = arg -> new TxnSetOperation();
        constructors[TXN_UNLOCK] = arg -> new TxnUnlockOperation();
        constructors[TXN_UNLOCK_BACKUP] = arg -> new TxnUnlockBackupOperation();
        constructors[IS_PARTITION_LOADED_FACTORY] = arg -> new IsPartitionLoadedOperationFactory();
        constructors[ADD_INDEX_FACTORY] = arg -> new AddIndexOperationFactory();
        constructors[CLEAR_FACTORY] = arg -> new ClearOperationFactory();
        constructors[CONTAINS_VALUE_FACTORY] = arg -> new ContainsValueOperationFactory();
        constructors[EVICT_ALL_FACTORY] = arg -> new EvictAllOperationFactory();
        constructors[IS_EMPTY_FACTORY] = arg -> new IsEmptyOperationFactory();
        constructors[KEY_LOAD_STATUS_FACTORY] = arg -> new KeyLoadStatusOperationFactory();
        constructors[MAP_FLUSH_FACTORY] = arg -> new MapFlushOperationFactory();
        constructors[MAP_GET_ALL_FACTORY] = arg -> new MapGetAllOperationFactory();
        constructors[LOAD_ALL_FACTORY] = arg -> new MapLoadAllOperationFactory();
        constructors[PARTITION_WIDE_ENTRY_FACTORY] = arg -> new PartitionWideEntryOperationFactory();
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_FACTORY] = arg -> new PartitionWideEntryWithPredicateOperationFactory();
        constructors[PUT_ALL_PARTITION_AWARE_FACTORY] = arg -> new PutAllPartitionAwareOperationFactory();
        constructors[SIZE_FACTORY] = arg -> new SizeOperationFactory();
        constructors[MULTIPLE_ENTRY_FACTORY] = arg -> new MultipleEntryOperationFactory();
        constructors[ENTRY_EVENT_FILTER] = arg -> new EntryEventFilter();
        constructors[EVENT_LISTENER_FILTER] = arg -> new EventListenerFilter();
        constructors[PARTITION_LOST_EVENT_FILTER] = arg -> new MapPartitionLostEventFilter();
        constructors[NEAR_CACHE_SINGLE_INVALIDATION] = arg -> new SingleNearCacheInvalidation();
        constructors[NEAR_CACHE_BATCH_INVALIDATION] = arg -> new BatchNearCacheInvalidation();
        constructors[ADD_INTERCEPTOR] = arg -> new AddInterceptorOperation();
        constructors[MAP_REPLICATION] = arg -> new MapReplicationOperation();
        constructors[POST_JOIN_MAP_OPERATION] = arg -> new PostJoinMapOperation();
        constructors[INDEX_INFO] = arg -> new IndexInfo();
        constructors[MAP_INDEX_INFO] = arg -> new MapIndexInfo();
        constructors[INTERCEPTOR_INFO] = arg -> new PostJoinMapOperation.InterceptorInfo();
        constructors[REMOVE_INTERCEPTOR] = arg -> new RemoveInterceptorOperation();
        constructors[QUERY_EVENT_FILTER] = arg -> new QueryEventFilter();
        constructors[RECORD_INFO] = arg -> new RecordInfo();
        constructors[RECORD_REPLICATION_INFO] = arg -> new RecordReplicationInfo();
        constructors[HIGHER_HITS_MERGE_POLICY] = arg -> new HigherHitsMapMergePolicy();
        constructors[LATEST_UPDATE_MERGE_POLICY] = arg -> new LatestUpdateMapMergePolicy();
        constructors[PASS_THROUGH_MERGE_POLICY] = arg -> new PassThroughMergePolicy();
        constructors[PUT_IF_ABSENT_MERGE_POLICY] = arg -> new PutIfAbsentMapMergePolicy();
        constructors[UUID_FILTER] = arg -> new UuidFilter();
        constructors[MAP_TRANSACTION_LOG_RECORD] = arg -> new MapTransactionLogRecord();
        constructors[VERSIONED_VALUE] = arg -> new VersionedValue();
        constructors[MAP_REPLICATION_STATE_HOLDER] = arg -> new MapReplicationStateHolder();
        constructors[WRITE_BEHIND_STATE_HOLDER] = arg -> new WriteBehindStateHolder();
        constructors[AGGREGATION_RESULT] = arg -> new AggregationResult();
        constructors[QUERY] = arg -> new Query();
        constructors[TARGET] = arg -> new Target();
        constructors[MAP_INVALIDATION_METADATA] = arg -> new MapGetInvalidationMetaDataOperation();
        constructors[MAP_INVALIDATION_METADATA_RESPONSE] = arg -> new MapGetInvalidationMetaDataOperation.MetaDataResponse();
        constructors[MAP_NEAR_CACHE_STATE_HOLDER] = arg -> new MapNearCacheStateHolder();
        constructors[MAP_ASSIGN_AND_GET_UUIDS_FACTORY] = arg -> new MapAssignAndGetUuidsOperationFactory();
        constructors[MAP_ASSIGN_AND_GET_UUIDS] = arg -> new MapAssignAndGetUuidsOperation();
        constructors[DESTROY_QUERY_CACHE] = arg -> new DestroyQueryCacheOperation();
        constructors[MADE_PUBLISHABLE] = arg -> new MadePublishableOperation();
        constructors[MADE_PUBLISHABLE_FACTORY] = arg -> new MadePublishableOperationFactory();
        constructors[PUBLISHER_CREATE] = arg -> new PublisherCreateOperation();
        constructors[READ_AND_RESET_ACCUMULATOR] = arg -> new ReadAndResetAccumulatorOperation();
        constructors[SET_READ_CURSOR] = arg -> new SetReadCursorOperation();
        constructors[ACCUMULATOR_CONSUMER] = arg -> new AccumulatorConsumerOperation();
        constructors[LAZY_MAP_ENTRY] = arg -> new LazyMapEntry();
        constructors[TRIGGER_LOAD_IF_NEEDED] = arg -> new TriggerLoadIfNeededOperation();
        constructors[IS_KEYLOAD_FINISHED] = arg -> new IsKeyLoadFinishedOperation();
        constructors[REMOVE_FROM_LOAD_ALL] = arg -> new RemoveFromLoadAllOperation();
        constructors[ENTRY_REMOVING_PROCESSOR] = arg -> EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR;
        constructors[ENTRY_OFFLOADABLE_SET_UNLOCK] = arg -> new EntryOffloadableSetUnlockOperation();
        constructors[LOCK_AWARE_LAZY_MAP_ENTRY] = arg -> new LockAwareLazyMapEntry();
        constructors[FETCH_WITH_QUERY] = arg -> new MapFetchWithQueryOperation();
        constructors[RESULT_SEGMENT] = arg -> new ResultSegment();
        constructors[EVICT_BATCH_BACKUP] = arg -> new EvictBatchBackupOperation();
        constructors[EVENT_JOURNAL_SUBSCRIBE_OPERATION] = arg -> new MapEventJournalSubscribeOperation();
        constructors[EVENT_JOURNAL_READ] = arg -> new MapEventJournalReadOperation<>();
        constructors[EVENT_JOURNAL_DESERIALIZING_MAP_EVENT] = arg -> new DeserializingEventJournalMapEvent<>();
        constructors[EVENT_JOURNAL_INTERNAL_MAP_EVENT] = arg -> new InternalEventJournalMapEvent();
        constructors[EVENT_JOURNAL_READ_RESULT_SET] = arg -> new MapEventJournalReadResultSetImpl<>();
        constructors[MERGE_FACTORY] = arg -> new MergeOperationFactory();
        constructors[MERGE] = arg -> new MergeOperation();
        constructors[SET_TTL] = arg -> new SetTtlOperation();
        constructors[SET_TTL_BACKUP] = arg -> new SetTtlBackupOperation();
        constructors[MERKLE_TREE_NODE_ENTRIES] = arg -> new MerkleTreeNodeEntries();

        return new ArrayDataSerializableFactory(constructors);
    }
}

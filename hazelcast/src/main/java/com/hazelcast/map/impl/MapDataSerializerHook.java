/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.nearcache.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.map.impl.nearcache.invalidation.UuidFilter;
import com.hazelcast.map.impl.operation.AddIndexOperation;
import com.hazelcast.map.impl.operation.AddIndexOperationFactory;
import com.hazelcast.map.impl.operation.AddInterceptorOperationFactory;
import com.hazelcast.map.impl.operation.AwaitMapFlushOperation;
import com.hazelcast.map.impl.operation.ClearBackupOperation;
import com.hazelcast.map.impl.operation.ClearNearCacheOperation;
import com.hazelcast.map.impl.operation.ClearOperation;
import com.hazelcast.map.impl.operation.ContainsKeyOperation;
import com.hazelcast.map.impl.operation.ContainsValueOperation;
import com.hazelcast.map.impl.operation.ContainsValueOperationFactory;
import com.hazelcast.map.impl.operation.DeleteOperation;
import com.hazelcast.map.impl.operation.EntryBackupOperation;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.map.impl.operation.EvictAllBackupOperation;
import com.hazelcast.map.impl.operation.EvictAllOperation;
import com.hazelcast.map.impl.operation.EvictAllOperationFactory;
import com.hazelcast.map.impl.operation.EvictBackupOperation;
import com.hazelcast.map.impl.operation.EvictOperation;
import com.hazelcast.map.impl.operation.GetAllOperation;
import com.hazelcast.map.impl.operation.GetEntryViewOperation;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.IsEmptyOperationFactory;
import com.hazelcast.map.impl.operation.LoadAllOperation;
import com.hazelcast.map.impl.operation.LoadMapOperation;
import com.hazelcast.map.impl.operation.LoadStatusOperation;
import com.hazelcast.map.impl.operation.LoadStatusOperationFactory;
import com.hazelcast.map.impl.operation.MapFetchEntriesOperation;
import com.hazelcast.map.impl.operation.MapFetchKeysOperation;
import com.hazelcast.map.impl.operation.MapFlushBackupOperation;
import com.hazelcast.map.impl.operation.MapFlushOperation;
import com.hazelcast.map.impl.operation.MapFlushOperationFactory;
import com.hazelcast.map.impl.operation.MapGetAllOperationFactory;
import com.hazelcast.map.impl.operation.MapIsEmptyOperation;
import com.hazelcast.map.impl.operation.MapLoadAllOperationFactory;
import com.hazelcast.map.impl.operation.MapSizeOperation;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.operation.MultipleEntryBackupOperation;
import com.hazelcast.map.impl.operation.MultipleEntryOperation;
import com.hazelcast.map.impl.operation.MultipleEntryOperationFactory;
import com.hazelcast.map.impl.operation.MultipleEntryWithPredicateBackupOperation;
import com.hazelcast.map.impl.operation.MultipleEntryWithPredicateOperation;
import com.hazelcast.map.impl.operation.NotifyMapFlushOperation;
import com.hazelcast.map.impl.operation.PartitionCheckIfLoadedOperation;
import com.hazelcast.map.impl.operation.PartitionCheckIfLoadedOperationFactory;
import com.hazelcast.map.impl.operation.PartitionWideEntryBackupOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryOperationFactory;
import com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateBackupOperation;
import com.hazelcast.map.impl.operation.PartitionWideEntryWithPredicateOperation;
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
import com.hazelcast.map.impl.operation.RemoveIfSameOperation;
import com.hazelcast.map.impl.operation.RemoveInterceptorOperationFactory;
import com.hazelcast.map.impl.operation.RemoveOperation;
import com.hazelcast.map.impl.operation.ReplaceIfSameOperation;
import com.hazelcast.map.impl.operation.ReplaceOperation;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.map.impl.operation.SizeOperationFactory;
import com.hazelcast.map.impl.operation.TryPutOperation;
import com.hazelcast.map.impl.operation.TryRemoveOperation;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.tx.TxnDeleteOperation;
import com.hazelcast.map.impl.tx.TxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.TxnPrepareBackupOperation;
import com.hazelcast.map.impl.tx.TxnPrepareOperation;
import com.hazelcast.map.impl.tx.TxnRollbackBackupOperation;
import com.hazelcast.map.impl.tx.TxnRollbackOperation;
import com.hazelcast.map.impl.tx.TxnSetOperation;
import com.hazelcast.map.impl.tx.TxnUnlockBackupOperation;
import com.hazelcast.map.impl.tx.TxnUnlockOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ClearOperationFactory;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MutableInteger;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_DS_FACTORY_ID;

public final class MapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MAP_DS_FACTORY, MAP_DS_FACTORY_ID);

    public static final MutableInteger ID = new MutableInteger();

    public static final int PUT = ID.value++;
    public static final int GET = ID.value++;
    public static final int REMOVE = ID.value++;
    public static final int PUT_BACKUP = ID.value++;
    public static final int REMOVE_BACKUP = ID.value++;
    public static final int KEY_SET = ID.value++;
    public static final int VALUES = ID.value++;
    public static final int ENTRIES = ID.value++;
    public static final int ENTRY_VIEW = ID.value++;
    public static final int QUERY_RESULT_ROW = ID.value++;
    public static final int QUERY_RESULT = ID.value++;
    public static final int EVICT_BACKUP = ID.value++;
    public static final int CONTAINS_KEY = ID.value++;
    public static final int KEYS_WITH_CURSOR = ID.value++;
    public static final int ENTRIES_WITH_CURSOR = ID.value++;
    public static final int SET = ID.value++;
    public static final int LOAD_MAP = ID.value++;
    public static final int LOAD_STATUS = ID.value++;
    public static final int LOAD_ALL = ID.value++;
    public static final int ENTRY_BACKUP = ID.value++;
    public static final int ENTRY_OPERATION = ID.value++;
    public static final int PUT_ALL = ID.value++;
    public static final int PUT_ALL_BACKUP = ID.value++;
    public static final int REMOVE_IF_SAME = ID.value++;
    public static final int REPLACE = ID.value++;
    public static final int SIZE = ID.value++;
    public static final int CLEAR_BACKUP = ID.value++;
    public static final int CLEAR_NEAR_CACHE = ID.value++;
    public static final int CLEAR = ID.value++;
    public static final int DELETE = ID.value++;
    public static final int EVICT = ID.value++;
    public static final int EVICT_ALL = ID.value++;
    public static final int EVICT_ALL_BACKUP = ID.value++;
    public static final int GET_ALL = ID.value++;
    public static final int IS_EMPTY = ID.value++;
    public static final int MERGE = ID.value++;
    public static final int CHECK_IF_LOADED = ID.value++;
    public static final int PARTITION_WIDE_ENTRY = ID.value++;
    public static final int PARTITION_WIDE_ENTRY_BACKUP = ID.value++;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY = ID.value++;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_BACKUP = ID.value++;
    public static final int ADD_INDEX = ID.value++;
    public static final int AWAIT_MAP_FLUSH = ID.value++;
    public static final int CONTAINS_VALUE = ID.value++;
    public static final int GET_ENTRY_VIEW = ID.value++;
    public static final int FETCH_ENTRIES = ID.value++;
    public static final int FETCH_KEYS = ID.value++;
    public static final int FLUSH_BACKUP = ID.value++;
    public static final int FLUSH = ID.value++;
    public static final int MULTIPLE_ENTRY_BACKUP = ID.value++;
    public static final int MULTIPLE_ENTRY = ID.value++;
    public static final int MULTIPLE_ENTRY_PREDICATE_BACKUP = ID.value++;
    public static final int MULTIPLE_ENTRY_PREDICATE = ID.value++;
    public static final int NOTIFY_MAP_FLUSH = ID.value++;
    public static final int PUT_IF_ABSENT = ID.value++;
    public static final int PUT_FROM_LOAD_ALL = ID.value++;
    public static final int PUT_FROM_LOAD_ALL_BACKUP = ID.value++;
    public static final int QUERY_PARTITION = ID.value++;
    public static final int QUERY = ID.value++;
    public static final int PUT_TRANSIENT = ID.value++;
    public static final int REPLACE_IF_SAME = ID.value++;
    public static final int TRY_PUT = ID.value++;
    public static final int TRY_REMOVE = ID.value++;
    public static final int TXN_LOCK_AND_GET = ID.value++;
    public static final int TXN_DELETE = ID.value++;
    public static final int TXN_PREPARE = ID.value++;
    public static final int TXN_PREPARE_BACKUP = ID.value++;
    public static final int TXN_ROLLBACK = ID.value++;
    public static final int TXN_ROLLBACK_BACKUP = ID.value++;
    public static final int TXN_SET = ID.value++;
    public static final int TXN_UNLOCK = ID.value++;
    public static final int TXN_UNLOCK_BACKUP = ID.value++;
    public static final int CHECK_IF_LOADED_FACTORY = ID.value++;
    public static final int ADD_INDEX_FACTORY = ID.value++;
    public static final int ADD_INTERCEPTOR_FACTORY = ID.value++;
    public static final int CLEAR_FACTORY = ID.value++;
    public static final int CONTAINS_VALUE_FACTORY = ID.value++;
    public static final int EVICT_ALL_FACTORY = ID.value++;
    public static final int IS_EMPTY_FACTORY = ID.value++;
    public static final int LOAD_STATUS_FACTORY = ID.value++;
    public static final int MAP_FLUSH_FACTORY = ID.value++;
    public static final int MAP_GET_ALL_FACTORY = ID.value++;
    public static final int LOAD_ALL_FACTORY = ID.value++;
    public static final int PARTITION_WIDE_ENTRY_FACTORY = ID.value++;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_FACTORY = ID.value++;
    public static final int PUT_ALL_PARTITION_AWARE_FACTORY = ID.value++;
    public static final int REMOVE_INTERCEPTOR_FACTORY = ID.value++;
    public static final int SIZE_FACTORY = ID.value++;
    public static final int MULTIPLE_ENTRY_FACTORY = ID.value++;
    public static final int NEAR_CACHE_SINGLE_INVALIDATION = ID.value++;
    public static final int NEAR_CACHE_BATCH_INVALIDATION = ID.value++;
    public static final int UUID_FILTER = ID.value++;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[ID.value];

        constructors[PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutOperation();
            }
        };
        constructors[GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveOperation();
            }
        };
        constructors[PUT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutBackupOperation();
            }
        };
        constructors[REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveBackupOperation();
            }
        };
        constructors[EVICT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EvictBackupOperation();
            }
        };
        constructors[KEY_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapKeySet();
            }
        };
        constructors[VALUES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapValueCollection();
            }
        };
        constructors[ENTRIES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapEntries();
            }
        };
        constructors[ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return (IdentifiedDataSerializable) EntryViews.createSimpleEntryView();
            }
        };
        constructors[QUERY_RESULT_ROW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueryResultRow();
            }
        };
        constructors[QUERY_RESULT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueryResult();
            }
        };
        constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsKeyOperation();
            }
        };
        constructors[KEYS_WITH_CURSOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapKeysWithCursor();
            }
        };
        constructors[ENTRIES_WITH_CURSOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapEntriesWithCursor();
            }
        };
        constructors[SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetOperation();
            }
        };
        constructors[LOAD_MAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LoadMapOperation();
            }
        };
        constructors[LOAD_STATUS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LoadStatusOperation();
            }
        };
        constructors[LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LoadAllOperation();
            }
        };
        constructors[ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EntryBackupOperation();
            }
        };
        constructors[ENTRY_OPERATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EntryOperation();
            }
        };
        constructors[PUT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutAllOperation();
            }
        };
        constructors[PUT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutAllBackupOperation();
            }
        };
        constructors[REMOVE_IF_SAME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveIfSameOperation();
            }
        };
        constructors[REPLACE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplaceOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapSizeOperation();
            }
        };
        constructors[CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearBackupOperation();
            }
        };
        constructors[CLEAR_NEAR_CACHE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearNearCacheOperation();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperation();
            }
        };
        constructors[DELETE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DeleteOperation();
            }
        };
        constructors[EVICT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EvictOperation();
            }
        };
        constructors[EVICT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EvictAllOperation();
            }
        };
        constructors[EVICT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EvictAllBackupOperation();
            }
        };
        constructors[GET_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetAllOperation();
            }
        };
        constructors[IS_EMPTY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapIsEmptyOperation();
            }
        };
        constructors[MERGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MergeOperation();
            }
        };
        constructors[CHECK_IF_LOADED] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionCheckIfLoadedOperation();
            }
        };
        constructors[PARTITION_WIDE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionWideEntryOperation();
            }
        };
        constructors[PARTITION_WIDE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionWideEntryBackupOperation();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionWideEntryWithPredicateOperation();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionWideEntryWithPredicateBackupOperation();
            }
        };
        constructors[ADD_INDEX] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddIndexOperation();
            }
        };
        constructors[AWAIT_MAP_FLUSH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AwaitMapFlushOperation();
            }
        };
        constructors[CONTAINS_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsValueOperation();
            }
        };
        constructors[GET_ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GetEntryViewOperation();
            }
        };
        constructors[FETCH_ENTRIES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapFetchEntriesOperation();
            }
        };
        constructors[FETCH_KEYS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapFetchKeysOperation();
            }
        };
        constructors[FLUSH_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapFlushBackupOperation();
            }
        };
        constructors[FLUSH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapFlushOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultipleEntryBackupOperation();
            }
        };
        constructors[MULTIPLE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultipleEntryOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_PREDICATE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultipleEntryWithPredicateBackupOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultipleEntryWithPredicateOperation();
            }
        };
        constructors[NOTIFY_MAP_FLUSH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NotifyMapFlushOperation();
            }
        };
        constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutIfAbsentOperation();
            }
        };
        constructors[PUT_FROM_LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutFromLoadAllOperation();
            }
        };
        constructors[PUT_FROM_LOAD_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutFromLoadAllBackupOperation();
            }
        };
        constructors[QUERY_PARTITION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueryPartitionOperation();
            }
        };
        constructors[QUERY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueryOperation();
            }
        };
        constructors[PUT_TRANSIENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutTransientOperation();
            }
        };
        constructors[REPLACE_IF_SAME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplaceIfSameOperation();
            }
        };
        constructors[TRY_PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TryPutOperation();
            }
        };
        constructors[TRY_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TryRemoveOperation();
            }
        };
        constructors[TXN_LOCK_AND_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnLockAndGetOperation();
            }
        };
        constructors[TXN_DELETE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnDeleteOperation();
            }
        };
        constructors[TXN_PREPARE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPrepareOperation();
            }
        };
        constructors[TXN_PREPARE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnPrepareBackupOperation();
            }
        };
        constructors[TXN_ROLLBACK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRollbackOperation();
            }
        };
        constructors[TXN_ROLLBACK_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnRollbackBackupOperation();
            }
        };
        constructors[TXN_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnSetOperation();
            }
        };
        constructors[TXN_UNLOCK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnUnlockOperation();
            }
        };
        constructors[TXN_UNLOCK_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TxnUnlockBackupOperation();
            }
        };
        constructors[CHECK_IF_LOADED_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionCheckIfLoadedOperationFactory();
            }
        };
        constructors[ADD_INDEX_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddIndexOperationFactory();
            }
        };
        constructors[ADD_INTERCEPTOR_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddInterceptorOperationFactory();
            }
        };
        constructors[CLEAR_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClearOperationFactory();
            }
        };
        constructors[CONTAINS_VALUE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ContainsValueOperationFactory();
            }
        };
        constructors[EVICT_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EvictAllOperationFactory();
            }
        };
        constructors[IS_EMPTY_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IsEmptyOperationFactory();
            }
        };
        constructors[LOAD_STATUS_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LoadStatusOperationFactory();
            }
        };
        constructors[MAP_FLUSH_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapFlushOperationFactory();
            }
        };
        constructors[MAP_GET_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapGetAllOperationFactory();
            }
        };
        constructors[LOAD_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapLoadAllOperationFactory();
            }
        };
        constructors[PARTITION_WIDE_ENTRY_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionWideEntryOperationFactory();
            }
        };
        constructors[PUT_ALL_PARTITION_AWARE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PutAllPartitionAwareOperationFactory();
            }
        };
        constructors[REMOVE_INTERCEPTOR_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RemoveInterceptorOperationFactory();
            }
        };
        constructors[SIZE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SizeOperationFactory();
            }
        };
        constructors[MULTIPLE_ENTRY_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultipleEntryOperationFactory();
            }
        };
        constructors[NEAR_CACHE_SINGLE_INVALIDATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SingleNearCacheInvalidation();
            }
        };
        constructors[NEAR_CACHE_BATCH_INVALIDATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BatchNearCacheInvalidation();
            }
        };
        constructors[UUID_FILTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new UuidFilter();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}

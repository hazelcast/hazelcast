/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.tx.TxnDeleteOperation;
import com.hazelcast.map.impl.tx.TxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.TxnSetOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.map.impl.record.Record.UNSET;
import static java.util.Collections.singletonList;

/**
 * Creates map operations.
 */
public class DefaultMapOperationProvider implements MapOperationProvider {

    public DefaultMapOperationProvider() {
    }

    @Override
    public OperationFactory createMapSizeOperationFactory(String name) {
        return new SizeOperationFactory(name);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        return new TryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        if (hasNoExpiry(ttl, maxIdle)) {
            return new PutOperation(name, key, value);
        } else {
            return new PutWithExpiryOperation(name, key, value, ttl, maxIdle);
        }
    }

    private static boolean hasNoExpiry(long ttl, long maxIdle) {
        return ttl == UNSET && maxIdle == UNSET;
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        if (hasNoExpiry(ttl, maxIdle)) {
            return new SetOperation(name, dataKey, value);
        } else {
            return new SetWithExpiryOperation(name, dataKey, value, ttl, maxIdle);
        }
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        if (hasNoExpiry(ttl, maxIdle)) {
            return new PutIfAbsentOperation(name, key, value);
        } else {
            return new PutIfAbsentWithExpiryOperation(name, key, value, ttl, maxIdle);
        }
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data keyData, Data valueData, long ttl, long maxIdle) {
        if (hasNoExpiry(ttl, maxIdle)) {
            return new PutTransientOperation(name, keyData, valueData);
        } else {
            return new PutTransientWithExpiryOperation(name, keyData, valueData, ttl, maxIdle);
        }
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key) {
        return new RemoveOperation(name, key);
    }

    @Override
    public MapOperation createSetTtlOperation(String name, Data key, long ttl) {
        return new SetTtlOperation(name, key, ttl);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        return new TryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createReplaceOperation(String name, Data dataKey, Data value) {
        return new ReplaceOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value) {
        return new RemoveIfSameOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        return new ReplaceIfSameOperation(name, dataKey, expect, update);
    }

    @Override
    public MapOperation createDeleteOperation(String name, Data key, boolean disableWanReplicationEvent) {
        return new DeleteOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createClearOperation(String name) {
        return new ClearOperation(name);
    }

    @Override
    public MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        return new EntryOperation(name, dataKey, entryProcessor);
    }

    @Override
    public MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        return new EvictOperation(name, dataKey, asyncBackup);
    }

    @Override
    public MapOperation createEvictAllOperation(String name) {
        return new EvictAllOperation(name);
    }

    @Override
    public MapOperation createContainsKeyOperation(String name, Data dataKey) {
        return new ContainsKeyOperation(name, dataKey);
    }

    @Override
    public OperationFactory createContainsValueOperationFactory(String name, Data testValue) {
        return new ContainsValueOperationFactory(name, testValue);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(String name, List<Data> keys) {
        return new MapGetAllOperationFactory(name, keys);
    }

    @Override
    public OperationFactory createEvictAllOperationFactory(String name) {
        return new EvictAllOperationFactory(name);
    }

    @Override
    public OperationFactory createClearOperationFactory(String name) {
        return new ClearOperationFactory(name);
    }

    @Override
    public OperationFactory createMapFlushOperationFactory(String name) {
        return new MapFlushOperationFactory(name);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(String name, List<Data> keys,
                                                          boolean replaceExistingValues) {
        return new MapLoadAllOperationFactory(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createGetEntryViewOperation(String name, Data dataKey) {
        return new GetEntryViewOperation(name, dataKey);
    }

    @Override
    public OperationFactory createPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        return new PartitionWideEntryOperationFactory(name, entryProcessor);
    }

    @Override
    public MapOperation createTxnDeleteOperation(String name, Data dataKey, long version) {
        return new TxnDeleteOperation(name, dataKey, version);
    }

    @Override
    public MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, UUID
            ownerUuid, boolean shouldLoad, boolean blockReads) {
        return new TxnLockAndGetOperation(name, dataKey, timeout, ttl, ownerUuid, shouldLoad, blockReads);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        return new TxnSetOperation(name, dataKey, value, version, ttl);
    }

    @Override
    public MapOperation createMergeOperation(String name, MapMergeTypes<Object, Object> mergingValue,
                                             SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy,
                                             boolean disableWanReplicationEvent) {
        return new MergeOperation(name, singletonList(mergingValue), mergePolicy, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createMapFlushOperation(String name) {
        return new MapFlushOperation(name);
    }

    @Override
    public MapOperation createLoadMapOperation(String name, boolean replaceExistingValues) {
        return new LoadMapOperation(name, replaceExistingValues);
    }

    @Override
    public OperationFactory createPartitionWideEntryWithPredicateOperationFactory(String name,
                                                                                  EntryProcessor entryProcessor,
                                                                                  Predicate predicate) {
        return new PartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
    }

    @Override
    public OperationFactory createMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor
            entryProcessor) {
        return new MultipleEntryOperationFactory(name, keys, entryProcessor);
    }

    @Override
    public MapOperation createGetOperation(String name, Data dataKey) {
        return new GetOperation(name, dataKey);
    }

    @Override
    public Operation createQueryOperation(Query query) {
        return new QueryOperation(query);
    }

    @Override
    public MapOperation createQueryPartitionOperation(Query query) {
        return new QueryPartitionOperation(query);
    }

    @Override
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        return new LoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries, boolean triggerMapLoader) {
        return new PutAllOperation(name, mapEntries, triggerMapLoader);
    }

    @Override
    public OperationFactory createPutAllOperationFactory(String name, int[] partitions,
                                                         MapEntries[] mapEntries, boolean triggerMapLoader) {
        return new PutAllPartitionAwareOperationFactory(name, partitions, mapEntries, triggerMapLoader);
    }

    @Override
    public OperationFactory createMergeOperationFactory(String name, int[] partitions,
                                                        List<MapMergeTypes<Object, Object>>[] mergingEntries,
                                                        SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>,
                                                                Object> mergePolicy) {
        return new MergeOperationFactory(name, partitions, mergingEntries, mergePolicy);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence, boolean includesExpirationTime) {
        return new PutFromLoadAllOperation(name, keyValueSequence, includesExpirationTime);
    }

    @Override
    public MapOperation createFetchKeysOperation(String name, IterationPointer[] pointers, int fetchSize) {
        return new MapFetchKeysOperation(name, pointers, fetchSize);
    }

    @Override
    public MapOperation createFetchEntriesOperation(String name, IterationPointer[] pointers, int fetchSize) {
        return new MapFetchEntriesOperation(name, pointers, fetchSize);
    }

    @Override
    public MapOperation createFetchIndexOperation(
            String mapName,
            String indexName,
            IndexIterationPointer[] pointers,
            PartitionIdSet partitionIdSet,
            int sizeLimit
    ) {
        return new MapFetchIndexOperation(mapName, indexName, pointers, partitionIdSet, sizeLimit);
    }

    @Override
    public MapOperation createFetchWithQueryOperation(String name, IterationPointer[] pointers, int fetchSize,
                                                      Query query) {
        return new MapFetchWithQueryOperation(name, pointers, fetchSize, query);
    }
}

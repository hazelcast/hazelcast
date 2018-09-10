/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.util.List;
import java.util.Set;

/**
 * Base class which basically delegates all method calls to underlying {@link MapOperationProvider}
 * to prevent extending classes to override all methods.
 * <p/>
 * See {@link WANAwareOperationProvider} for an example.
 */
abstract class MapOperationProviderDelegator implements MapOperationProvider {

    abstract MapOperationProvider getDelegate();

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return getDelegate().createPutOperation(name, key, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        return getDelegate().createTryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        return getDelegate().createSetOperation(name, dataKey, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return getDelegate().createPutIfAbsentOperation(name, key, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return getDelegate().createPutTransientOperation(name, key, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key, boolean disableWanReplicationEvent) {
        return getDelegate().createRemoveOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        return getDelegate().createTryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createReplaceOperation(String name, Data dataKey, Data value) {
        return getDelegate().createReplaceOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value) {
        return getDelegate().createRemoveIfSameOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        return getDelegate().createReplaceIfSameOperation(name, dataKey, expect, update);
    }

    @Override
    public MapOperation createDeleteOperation(String name, Data key, boolean disableWanReplicationEvent) {
        return getDelegate().createDeleteOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createClearOperation(String name) {
        return getDelegate().createClearOperation(name);
    }

    @Override
    public MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        return getDelegate().createEntryOperation(name, dataKey, entryProcessor);
    }

    @Override
    public MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        return getDelegate().createEvictOperation(name, dataKey, asyncBackup);
    }

    @Override
    public MapOperation createEvictAllOperation(String name) {
        return getDelegate().createEvictAllOperation(name);
    }

    @Override
    public MapOperation createContainsKeyOperation(String name, Data dataKey) {
        return getDelegate().createContainsKeyOperation(name, dataKey);
    }

    @Override
    public MapOperation createGetEntryViewOperation(String name, Data dataKey) {
        return getDelegate().createGetEntryViewOperation(name, dataKey);
    }

    @Override
    public MapOperation createGetOperation(String name, Data dataKey) {
        return getDelegate().createGetOperation(name, dataKey);
    }

    @Override
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        return getDelegate().createLoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries) {
        return getDelegate().createPutAllOperation(name, mapEntries);
    }

    @Override
    public OperationFactory createPutAllOperationFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        return getDelegate().createPutAllOperationFactory(name, partitions, mapEntries);
    }

    @Override
    public OperationFactory createMergeOperationFactory(String name, int[] partitions, List<MapMergeTypes>[] mergingEntries,
                                                        SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy) {
        return getDelegate().createMergeOperationFactory(name, partitions, mergingEntries, mergePolicy);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        return getDelegate().createPutFromLoadAllOperation(name, keyValueSequence);
    }

    @Override
    public MapOperation createTxnDeleteOperation(String name, Data dataKey, long version) {
        return getDelegate().createTxnDeleteOperation(name, dataKey, version);
    }

    @Override
    public MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout,
                                                     long ttl, String ownerUuid, boolean shouldLoad, boolean blockReads) {
        return getDelegate().createTxnLockAndGetOperation(name, dataKey, timeout, ttl, ownerUuid, shouldLoad, blockReads);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        return getDelegate().createTxnSetOperation(name, dataKey, value, version, ttl);
    }

    @Override
    public MapOperation createSetTtlOperation(String name, Data key, long ttl) {
        return getDelegate().createSetTtlOperation(name, key, ttl);
    }

    @Override
    public MapOperation createLegacyMergeOperation(String name, EntryView<Data, Data> mergingEntry,
                                                   MapMergePolicy policy, boolean disableWanReplicationEvent) {
        return getDelegate().createLegacyMergeOperation(name, mergingEntry, policy, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createMergeOperation(String name, MapMergeTypes mergingValue,
                                             SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy,
                                             boolean disableWanReplicationEvent) {
        return getDelegate().createMergeOperation(name, mergingValue, mergePolicy, disableWanReplicationEvent);
    }

    @Override
    public OperationFactory createPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        return getDelegate().createPartitionWideEntryOperationFactory(name, entryProcessor);
    }

    @Override
    public OperationFactory createPartitionWideEntryWithPredicateOperationFactory(String name,
                                                                                  EntryProcessor entryProcessor,
                                                                                  Predicate predicate) {
        return getDelegate().createPartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
    }

    @Override
    public OperationFactory createMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        return getDelegate().createMultipleEntryOperationFactory(name, keys, entryProcessor);
    }

    @Override
    public OperationFactory createContainsValueOperationFactory(String name, Data testValue) {
        return getDelegate().createContainsValueOperationFactory(name, testValue);
    }

    @Override
    public OperationFactory createEvictAllOperationFactory(String name) {
        return getDelegate().createEvictAllOperationFactory(name);
    }

    @Override
    public OperationFactory createClearOperationFactory(String name) {
        return getDelegate().createClearOperationFactory(name);
    }

    @Override
    public OperationFactory createMapFlushOperationFactory(String name) {
        return getDelegate().createMapFlushOperationFactory(name);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(String name, List<Data> keys, boolean replaceExistingValues) {
        return getDelegate().createLoadAllOperationFactory(name, keys, replaceExistingValues);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(String name, List<Data> keys) {
        return getDelegate().createGetAllOperationFactory(name, keys);
    }

    @Override
    public OperationFactory createMapSizeOperationFactory(String name) {
        return getDelegate().createMapSizeOperationFactory(name);
    }

    @Override
    public MapOperation createMapFlushOperation(String name) {
        return getDelegate().createMapFlushOperation(name);
    }

    @Override
    public MapOperation createLoadMapOperation(String name, boolean replaceExistingValues) {
        return getDelegate().createLoadMapOperation(name, replaceExistingValues);
    }

    @Override
    public MapOperation createFetchKeysOperation(String name, int lastTableIndex, int fetchSize) {
        return getDelegate().createFetchKeysOperation(name, lastTableIndex, fetchSize);
    }

    @Override
    public MapOperation createFetchEntriesOperation(String name, int lastTableIndex, int fetchSize) {
        return getDelegate().createFetchEntriesOperation(name, lastTableIndex, fetchSize);
    }

    @Override
    public MapOperation createQueryOperation(Query query) {
        return getDelegate().createQueryOperation(query);
    }

    @Override
    public MapOperation createQueryPartitionOperation(Query query) {
        return getDelegate().createQueryPartitionOperation(query);
    }

    @Override
    public MapOperation createFetchWithQueryOperation(String name, int lastTableIndex, int fetchSize, Query query) {
        return getDelegate().createFetchWithQueryOperation(name, lastTableIndex, fetchSize, query);
    }
}

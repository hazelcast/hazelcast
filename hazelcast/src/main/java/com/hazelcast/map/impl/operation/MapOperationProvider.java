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
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Provides {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} specific
 * operations for {@link IMap IMap}.
 */
public interface MapOperationProvider {

    MapOperation createPutOperation(String name, Data key, Data value, long ttl, long maxIdle);

    MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout);

    MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl, long maxIdle);

    MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle);

    MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl, long maxIdle);

    MapOperation createSetTtlOperation(String name, Data key, long ttl);

    MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout);

    MapOperation createReplaceOperation(String name, Data dataKey, Data value);

    MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value);

    MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update);

    MapOperation createRemoveOperation(String name, Data key);

    /**
     * Creates a delete operation for an entry with key equal to {@code key} from the map named {@code name}.
     * You can also specify whether this operation should trigger a WAN replication event.
     *
     * @param name                       the map name
     * @param key                        the entry key
     * @param disableWanReplicationEvent if the delete operation should not send a WAN replication event
     * @return the delete operation
     */
    MapOperation createDeleteOperation(String name, Data key, boolean disableWanReplicationEvent);

    MapOperation createClearOperation(String name);

    MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor);

    MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup);

    MapOperation createEvictAllOperation(String name);

    MapOperation createContainsKeyOperation(String name, Data dataKey);

    MapOperation createGetEntryViewOperation(String name, Data dataKey);

    MapOperation createGetOperation(String name, Data dataKey);

    Operation createQueryOperation(Query query);

    MapOperation createQueryPartitionOperation(Query query);

    /**
     * Creates an operation to load entry values for the provided {@code keys} on
     * the partition owner.
     *
     * @param name                  the map name
     * @param keys                  the keys for which values are to be loaded
     * @param replaceExistingValues if the existing entries for the loaded keys should be replaced
     * @return the operation for triggering entry value loading
     */
    MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues);

    MapOperation createPutAllOperation(String name, MapEntries mapEntries, boolean triggerMapLoader);

    MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence, boolean expirationTime);

    MapOperation createTxnDeleteOperation(String name, Data dataKey, long version);

    MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, UUID ownerUuid,
                                              boolean shouldLoad, boolean blockReads);

    MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl);

    MapOperation createMergeOperation(String name, MapMergeTypes<Object, Object> mergingValue,
                                      SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy,
                                      boolean disableWanReplicationEvent);

    MapOperation createMapFlushOperation(String name);

    MapOperation createLoadMapOperation(String name, boolean replaceExistingValues);

    /**
     * Creates an operation for fetching a segment of a keys from a single
     * partition.
     *
     * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator(int, int, boolean)
     */
    MapOperation createFetchKeysOperation(String name, IterationPointer[] pointers, int fetchSize);

    /**
     * Creates an operation for fetching a segment of a entries from a single
     * partition.
     *
     * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator(int, int, boolean)
     */
    MapOperation createFetchEntriesOperation(String name, IterationPointer[] pointers, int fetchSize);

    /**
     * Creates an operation for fetching entries using an index.
     */
    MapOperation createFetchIndexOperation(String mapName,
                                           String indexName,
                                           IndexIterationPointer[] pointers,
                                           PartitionIdSet partitionIdSet,
                                           int sizeLimit);

    /**
     * Creates an operation for fetching a segment of a query result from a
     * single partition.
     *
     * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator(int, int, com.hazelcast.projection.Projection, Predicate)
     * @since 3.9
     */
    MapOperation createFetchWithQueryOperation(String name, IterationPointer[] pointers, int fetchSize, Query query);

    OperationFactory createPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor);

    OperationFactory createPartitionWideEntryWithPredicateOperationFactory(String name,
                                                                           EntryProcessor entryProcessor,
                                                                           Predicate predicate);

    OperationFactory createMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor);

    OperationFactory createContainsValueOperationFactory(String name, Data testValue);

    OperationFactory createEvictAllOperationFactory(String name);

    OperationFactory createClearOperationFactory(String name);

    OperationFactory createMapFlushOperationFactory(String name);

    OperationFactory createLoadAllOperationFactory(String name, List<Data> keys, boolean replaceExistingValues);

    OperationFactory createGetAllOperationFactory(String name, List<Data> keys);

    OperationFactory createMapSizeOperationFactory(String name);

    OperationFactory createPutAllOperationFactory(String name, int[] partitions,
                                                  MapEntries[] mapEntries, boolean triggerMapLoader);

    OperationFactory createMergeOperationFactory(String name, int[] partitions,
                                                 List<MapMergeTypes<Object, Object>>[] mergingEntries,
                                                 SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>,
                                                         Object> mergePolicy);
}

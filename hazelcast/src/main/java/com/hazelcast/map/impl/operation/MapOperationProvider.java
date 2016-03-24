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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;

import java.util.List;
import java.util.Set;

/**
 * Provides {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} specific
 * operations for {@link com.hazelcast.core.IMap IMap}.
 */
public interface MapOperationProvider {

    MapOperation createPutOperation(String name, Data key, Data value, long ttl);

    MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout);

    MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl);

    MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl);

    MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl);

    MapOperation createRemoveOperation(String name, Data key, boolean disableWanReplicationEvent);

    MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout);

    MapOperation createReplaceOperation(String name, Data dataKey, Data value);

    MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value);

    MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update);

    MapOperation createDeleteOperation(String name, Data key);

    MapOperation createClearOperation(String name);

    MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor);

    MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup);

    MapOperation createEvictAllOperation(String name);

    MapOperation createContainsKeyOperation(String name, Data dataKey);

    MapOperation createGetEntryViewOperation(String name, Data dataKey);

    MapOperation createGetOperation(String name, Data dataKey);

    MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues);

    MapOperation createPutAllOperation(String name, MapEntries mapEntries);

    MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence);

    MapOperation createTxnDeleteOperation(String name, Data dataKey, long version);

    MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, String ownerUuid,
                                              boolean shouldLoad, boolean blockReads);

    MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl);

    MapOperation createMergeOperation(String name, Data dataKey, EntryView<Data, Data> entryView,
                                      MapMergePolicy policy, boolean disableWanReplicationEvent);

    MapOperation createMapFlushOperation(String name);

    MapOperation createLoadMapOperation(String name, boolean replaceExistingValues);


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
}


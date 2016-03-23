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
import com.hazelcast.map.impl.tx.TxnDeleteOperation;
import com.hazelcast.map.impl.tx.TxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.TxnSetOperation;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;

import java.util.List;
import java.util.Set;

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
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl) {
        return new PutOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        return new TryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl) {
        return new SetOperation(name, dataKey, value, ttl);
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl) {
        return new PutIfAbsentOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl) {
        return new PutTransientOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key, boolean disableWanReplicationEvent) {
        return new RemoveOperation(name, key, disableWanReplicationEvent);
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
    public MapOperation createDeleteOperation(String name, Data key) {
        return new DeleteOperation(name, key);
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
    public MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, String
            ownerUuid, boolean shouldLoad, boolean blockReads) {
        return new TxnLockAndGetOperation(name, dataKey, timeout, ttl, ownerUuid, shouldLoad, blockReads);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        return new TxnSetOperation(name, dataKey, value, version, ttl);
    }

    @Override
    public MapOperation createMergeOperation(String name, Data dataKey, EntryView<Data, Data> entryView,
                                             MapMergePolicy policy, boolean disableWanReplicationEvent) {
        return new MergeOperation(name, dataKey, entryView, policy, disableWanReplicationEvent);
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
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        return new LoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries) {
        return new PutAllOperation(name, mapEntries);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        return new PutFromLoadAllOperation(name, keyValueSequence);
    }
}

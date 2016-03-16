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

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;

import java.util.List;
import java.util.Set;

/**
 * This class is responsible for WAN replication related checks for supported mutating operations.
 * {@link com.hazelcast.spi.Operation} creations are delegated to underlying {@link MapOperationProvider} instance
 * after checks.
 */
public class WANAwareOperationProvider extends MapOperationProviderDelegator {

    private final MapServiceContext mapServiceContext;
    private final MapOperationProvider operationProviderDelegate;

    public WANAwareOperationProvider(MapServiceContext mapServiceContext, MapOperationProvider operationProviderDelegate) {
        this.mapServiceContext = mapServiceContext;
        this.operationProviderDelegate = operationProviderDelegate;
    }

    @Override
    MapOperationProvider getDelegate() {
        return operationProviderDelegate;
    }

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl) {
        checkWanReplicationQueues(name);
        return getDelegate().createPutOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        checkWanReplicationQueues(name);
        return getDelegate().createTryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl) {
        checkWanReplicationQueues(name);
        return getDelegate().createSetOperation(name, dataKey, value, ttl);
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl) {
        checkWanReplicationQueues(name);
        return getDelegate().createPutIfAbsentOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl) {
        checkWanReplicationQueues(name);
        return getDelegate().createPutTransientOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key, boolean disableWanReplicationEvent) {
        checkWanReplicationQueues(name);
        return getDelegate().createRemoveOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        checkWanReplicationQueues(name);
        return getDelegate().createTryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createReplaceOperation(String name, Data dataKey, Data value) {
        checkWanReplicationQueues(name);
        return getDelegate().createReplaceOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value) {
        checkWanReplicationQueues(name);
        return getDelegate().createRemoveIfSameOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        checkWanReplicationQueues(name);
        return getDelegate().createReplaceIfSameOperation(name, dataKey, expect, update);
    }

    @Override
    public MapOperation createDeleteOperation(String name, Data key) {
        checkWanReplicationQueues(name);
        return getDelegate().createDeleteOperation(name, key);
    }

    @Override
    public MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        checkWanReplicationQueues(name);
        return getDelegate().createEntryOperation(name, dataKey, entryProcessor);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries) {
        checkWanReplicationQueues(name);
        return getDelegate().createPutAllOperation(name, mapEntries);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        checkWanReplicationQueues(name);
        return getDelegate().createPutFromLoadAllOperation(name, keyValueSequence);
    }

    @Override
    public MapOperation createTxnDeleteOperation(String name, Data dataKey, long version) {
        checkWanReplicationQueues(name);
        return getDelegate().createTxnDeleteOperation(name, dataKey, version);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        checkWanReplicationQueues(name);
        return getDelegate().createTxnSetOperation(name, dataKey, value, version, ttl);
    }

    @Override
    public OperationFactory createPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        checkWanReplicationQueues(name);
        return getDelegate().createPartitionWideEntryOperationFactory(name, entryProcessor);
    }

    @Override
    public OperationFactory createPartitionWideEntryWithPredicateOperationFactory(String name,
                                                                                  EntryProcessor entryProcessor,
                                                                                  Predicate predicate) {
        checkWanReplicationQueues(name);
        return getDelegate()
                .createPartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
    }

    @Override
    public OperationFactory createMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        checkWanReplicationQueues(name);
        return getDelegate().createMultipleEntryOperationFactory(name, keys, entryProcessor);
    }

    private void checkWanReplicationQueues(String name) {
        mapServiceContext.getMapContainer(name).checkWanReplicationQueues();
    }
}

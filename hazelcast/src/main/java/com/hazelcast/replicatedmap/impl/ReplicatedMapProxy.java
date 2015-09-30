/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntrySet;
import com.hazelcast.replicatedmap.impl.operation.PutAllOperation;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.RemoveOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.replicatedmap.impl.record.ReplicatedEntryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.eventservice.impl.EmptyFilter;
import com.hazelcast.util.ExceptionUtil;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link com.hazelcast.core.ReplicatedMap} interface.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedMapProxy<K, V> extends AbstractDistributedObject
        implements ReplicatedMap<K, V>, InitializingObject {

    private final String name;
    private final NodeEngine nodeEngine;
    private final ReplicatedMapService service;
    private final SerializationService serializationService;
    private final InternalPartitionServiceImpl partitionService;
    private final ReplicatedMapConfig config;

    ReplicatedMapProxy(NodeEngine nodeEngine, String name, ReplicatedMapService service) {
        super(nodeEngine, service);
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.config = service.getReplicatedMapConfig(name);
    }

    @Override
    public void initialize() {
        service.initializeListeners(name);
        if (nodeEngine.getClusterService().getSize() == 1) {
            return;
        }
        fireMapDataLoadingTasks();
        if (!config.isAsyncFillup()) {
            Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(name);
            for (ReplicatedRecordStore store : stores) {
                while (!store.isLoaded()) {
                    continue;
                }
            }
        }
    }

    private void fireMapDataLoadingTasks() {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            Address thisAddress = nodeEngine.getThisAddress();
            Address ownerAddress = partitionService.getPartitionOwner(i);
            if (thisAddress.equals(ownerAddress)) {
                continue;
            }
            RequestMapDataOperation requestMapDataOperation = new RequestMapDataOperation(name);
            OperationService operationService = nodeEngine.getOperationService();
            operationService.invokeOnPartition(SERVICE_NAME, requestMapDataOperation, i);
        }
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        return getName();
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public int size() {
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        int size = 0;
        for (ReplicatedRecordStore store : stores) {
            size += store.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        for (ReplicatedRecordStore store : stores) {
            if (!store.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        isNotNull(key, "key");
        int partitionId = partitionService.getPartitionId(key);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, partitionId);
        return store.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        isNotNull(value, "value");
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        for (ReplicatedRecordStore store : stores) {
            if (store.containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        isNotNull(key, "key");
        int partitionId = partitionService.getPartitionId(key);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(getName(), false, partitionId);
        return (V) store.get(key);
    }

    @Override
    public V put(K key, V value) {
        isNotNull(key, "key must not be null!");
        isNotNull(value, "value must not be null!");
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
        PutOperation putOperation = new PutOperation(getName(), dataKey, dataValue);
        InternalCompletableFuture<Object> future = getOperationService()
                .invokeOnPartition(getServiceName(), putOperation, partitionId);
        VersionResponsePair result = (VersionResponsePair) future.getSafely();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        isNotNull(key, "key must not be null!");
        isNotNull(value, "value must not be null!");
        isNotNull(timeUnit, "timeUnit");
        if (ttl < 0) {
            throw new IllegalArgumentException("ttl must be a positive integer");
        }
        long ttlMillis = timeUnit.toMillis(ttl);
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        int partitionId = partitionService.getPartitionId(dataKey);
        PutOperation putOperation = new PutOperation(getName(), dataKey, dataValue, ttlMillis);
        InternalCompletableFuture<Object> future = getOperationService()
                .invokeOnPartition(getServiceName(), putOperation, partitionId);
        VersionResponsePair result = (VersionResponsePair) future.getSafely();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public V remove(Object key) {
        isNotNull(key, "key");
        Data dataKey = nodeEngine.toData(key);
        int partitionId = partitionService.getPartitionId(key);
        RemoveOperation removeOperation = new RemoveOperation(getName(), dataKey);
        InternalCompletableFuture<Object> future = getOperationService()
                .invokeOnPartition(getServiceName(), removeOperation, partitionId);
        VersionResponsePair result = (VersionResponsePair) future.getSafely();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        checkNotNull(entries, "entries cannot be null");
        int partitionCount = partitionService.getPartitionCount();
        try {
            List<Future> futures = new ArrayList<Future>(partitionCount);
            ReplicatedMapEntrySet[] entrySetPerPartition = new ReplicatedMapEntrySet[partitionCount];

            // first we fill entrySetPerPartition
            for (Entry entry : entries.entrySet()) {
                isNotNull(entry.getKey(), "key must not be null!");
                isNotNull(entry.getValue(), "value must not be null!");

                int partitionId = partitionService.getPartitionId(entry.getKey());
                ReplicatedMapEntrySet entrySet = entrySetPerPartition[partitionId];
                if (entrySet == null) {
                    entrySet = new ReplicatedMapEntrySet();
                    entrySetPerPartition[partitionId] = entrySet;
                }

                Data keyData = serializationService.toData(entry.getKey());
                Data valueData = serializationService.toData(entry.getValue());
                entrySet.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, valueData));
            }

            // then we invoke the operations
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                ReplicatedMapEntrySet entrySet = entrySetPerPartition[partitionId];
                if (entrySet != null) {
                    // If there is a single entry, we could make use of a PutOperation since that is a bit cheaper
                    Future f = createPutAllOperationFuture(name, entrySet, partitionId);
                    futures.add(f);
                }
            }

            // then we sync on completion of these operations
            for (Future future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Future createPutAllOperationFuture(final String name, ReplicatedMapEntrySet entrySet, int partitionId) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new PutAllOperation(name, entrySet).setPartitionId(partitionId);
        return operationService.invokeOnPartition(SERVICE_NAME, op, partitionId);
    }


    @Override
    public void clear() {
        service.clearLocalAndRemoteRecordStores(name);
    }

    @Override
    public boolean removeEntryListener(String id) {
        return service.removeEventListener(name, id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        isNotNull(listener, "listener");
        return service.addEventListener(listener, new EmptyFilter(), name);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedEntryEventFilter(serializationService.toData(key));
        return service.addEventListener(listener, eventFilter, name);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(null, predicate);
        return service.addEventListener(listener, eventFilter, name);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(serializationService.toData(key), predicate);
        return service.addEventListener(listener, eventFilter, name);
    }

    @Override
    public Set<K> keySet() {
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Set<K> keySet = new HashSet<K>();
        for (ReplicatedRecordStore store : stores) {
            keySet.addAll(store.keySet(true));
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Collection<V> values = new ArrayList<V>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(true));
        }
        return values;
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        List<V> values = new ArrayList<V>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(comparator));
        }
        Collections.sort(values, comparator);
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Set<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>();
        for (ReplicatedRecordStore store : stores) {
            entrySet.addAll(store.entrySet(true));
        }
        return entrySet;
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + name;
    }


    public LocalReplicatedMapStats getReplicatedMapStats() {
        return service.createReplicatedMapStats(name);
    }

}

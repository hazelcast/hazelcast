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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.util.ResultSet;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntries;
import com.hazelcast.replicatedmap.impl.operation.ClearOperationFactory;
import com.hazelcast.replicatedmap.impl.operation.PutAllOperation;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.RemoveOperation;
import com.hazelcast.replicatedmap.impl.operation.RequestMapDataOperation;
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
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.IterationType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.lang.Thread.currentThread;

/**
 * Proxy implementation of {@link com.hazelcast.core.ReplicatedMap} interface.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings("checkstyle:methodcount")
public class ReplicatedMapProxy<K, V> extends AbstractDistributedObject<ReplicatedMapService>
        implements ReplicatedMap<K, V>, InitializingObject {

    private static final int WAIT_INTERVAL_MILLIS = 1000;
    private static final int RETRY_INTERVAL_COUNT = 3;
    private static final int KEY_SET_MIN_SIZE = 16;
    private static final int KEY_SET_STORE_MULTIPLE = 4;

    private final String name;
    private final NodeEngine nodeEngine;
    private final ReplicatedMapService service;
    private final ReplicatedMapEventPublishingService eventPublishingService;
    private final SerializationService serializationService;
    private final InternalPartitionServiceImpl partitionService;
    private final ReplicatedMapConfig config;

    private int retryCount;

    ReplicatedMapProxy(NodeEngine nodeEngine, String name, ReplicatedMapService service, ReplicatedMapConfig config) {
        super(nodeEngine, service);
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.eventPublishingService = service.getEventPublishingService();
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.config = config;
    }

    @Override
    public void initialize() {
        service.initializeListeners(name);
        if (nodeEngine.getClusterService().getSize() == 1) {
            return;
        }
        fireMapDataLoadingTasks();
        if (!config.isAsyncFillup()) {
            for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
                ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, i);
                while (store == null || !store.isLoaded()) {
                    if ((retryCount++) % RETRY_INTERVAL_COUNT == 0) {
                        requestDataForPartition(i);
                    }
                    sleep();
                }
            }
        }
    }

    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw rethrow(e);
        }
    }

    private void fireMapDataLoadingTasks() {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            requestDataForPartition(i);
        }
    }

    private void requestDataForPartition(int partitionId) {
        RequestMapDataOperation requestMapDataOperation = new RequestMapDataOperation(name);
        OperationService operationService = nodeEngine.getOperationService();
        operationService
                .createInvocationBuilder(SERVICE_NAME, requestMapDataOperation, partitionId)
                .setTryCount(ReplicatedMapService.INVOCATION_TRY_COUNT)
                .invoke();
    }

    @Override
    protected boolean preDestroy() {
        if (super.preDestroy()) {
            eventPublishingService.fireMapClearedEvent(size(), name);
            return true;
        }
        return false;
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
        ensureQuorumPresent(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        int size = 0;
        for (ReplicatedRecordStore store : stores) {
            size += store.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        ensureQuorumPresent(READ);
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
        ensureQuorumPresent(READ);
        isNotNull(key, "key");
        int partitionId = partitionService.getPartitionId(key);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, partitionId);
        return store != null && store.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        ensureQuorumPresent(READ);
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
        ensureQuorumPresent(READ);
        isNotNull(key, "key");
        int partitionId = partitionService.getPartitionId(key);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(getName(), false, partitionId);
        if (store == null) {
            return null;
        }
        return (V) store.get(key);
    }

    @Override
    public V put(K key, V value) {
        isNotNull(key, "key");
        isNotNull(value, "value");
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
        PutOperation putOperation = new PutOperation(getName(), dataKey, dataValue);
        InternalCompletableFuture<Object> future = getOperationService()
                .invokeOnPartition(getServiceName(), putOperation, partitionId);
        VersionResponsePair result = (VersionResponsePair) future.join();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        isNotNull(key, "key");
        isNotNull(value, "value");
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
        VersionResponsePair result = (VersionResponsePair) future.join();
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
        VersionResponsePair result = (VersionResponsePair) future.join();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        checkNotNull(entries, "entries cannot be null");
        int mapSize = entries.size();
        if (mapSize == 0) {
            return;
        }

        int partitionCount = partitionService.getPartitionCount();
        int initialSize = getPutAllInitialSize(mapSize, partitionCount);

        try {
            List<Future> futures = new ArrayList<Future>(partitionCount);
            ReplicatedMapEntries[] entrySetPerPartition = new ReplicatedMapEntries[partitionCount];

            // first we fill entrySetPerPartition
            for (Entry entry : entries.entrySet()) {
                isNotNull(entry.getKey(), "key");
                isNotNull(entry.getValue(), "value");

                int partitionId = partitionService.getPartitionId(entry.getKey());
                ReplicatedMapEntries mapEntries = entrySetPerPartition[partitionId];
                if (mapEntries == null) {
                    mapEntries = new ReplicatedMapEntries(initialSize);
                    entrySetPerPartition[partitionId] = mapEntries;
                }

                Data keyData = serializationService.toData(entry.getKey());
                Data valueData = serializationService.toData(entry.getValue());
                mapEntries.add(keyData, valueData);
            }

            // then we invoke the operations
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                ReplicatedMapEntries entrySet = entrySetPerPartition[partitionId];
                if (entrySet != null) {
                    Future future = createPutAllOperationFuture(name, entrySet, partitionId);
                    futures.add(future);
                }
            }

            // then we sync on completion of these operations
            for (Future future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int getPutAllInitialSize(int mapSize, int partitionCount) {
        if (mapSize == 1) {
            return 1;
        }
        // this is an educated guess for the initial size of the entries per partition, depending on the map size
        return (int) ceil(20f * mapSize / partitionCount / log10(mapSize));
    }

    private Future createPutAllOperationFuture(String name, ReplicatedMapEntries entrySet, int partitionId) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new PutAllOperation(name, entrySet);
        return operationService.invokeOnPartition(SERVICE_NAME, op, partitionId);
    }

    @Override
    public void clear() {
        OperationService operationService = nodeEngine.getOperationService();
        try {
            Map<Integer, Object> results = operationService.invokeOnAllPartitions(SERVICE_NAME, new ClearOperationFactory(name));
            int deletedEntrySize = 0;
            for (Object deletedEntryPerPartition : results.values()) {
                deletedEntrySize += (Integer) deletedEntryPerPartition;
            }
            eventPublishingService.fireMapClearedEvent(deletedEntrySize, name);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public boolean removeEntryListener(String id) {
        return eventPublishingService.removeEventListener(name, id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        isNotNull(listener, "listener");
        return eventPublishingService.addEventListener(listener, TrueEventFilter.INSTANCE, name);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedEntryEventFilter(serializationService.toData(key));
        return eventPublishingService.addEventListener(listener, eventFilter, name);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        isNotNull(listener, "listener");
        isNotNull(predicate, "predicate");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(null, predicate);
        return eventPublishingService.addEventListener(listener, eventFilter, name);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        isNotNull(listener, "listener");
        isNotNull(predicate, "predicate");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(serializationService.toData(key), predicate);
        return eventPublishingService.addEventListener(listener, eventFilter, name);
    }

    @Override
    public Set<K> keySet() {
        ensureQuorumPresent(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Set<K> keySet = createHashSet(Math.max(KEY_SET_MIN_SIZE, stores.size() * KEY_SET_STORE_MULTIPLE));
        for (ReplicatedRecordStore store : stores) {
            keySet.addAll(store.keySet(true));
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        ensureQuorumPresent(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Collection<V> values = new ArrayList<V>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(true));
        }
        return values;
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        ensureQuorumPresent(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        List<V> values = new ArrayList<V>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(comparator));
        }
        Collections.sort(values, comparator);
        return values;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Entry<K, V>> entrySet() {
        ensureQuorumPresent(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        List<Entry> entries = new ArrayList<Entry>();
        for (ReplicatedRecordStore store : stores) {
            entries.addAll(store.entrySet(true));
        }
        Set result = new ResultSet(entries, IterationType.ENTRY);
        return result;
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

    private void ensureQuorumPresent(QuorumType requiredQuorumPermissionType) {
        service.ensureQuorumPresent(name, requiredQuorumPermissionType);
    }
}

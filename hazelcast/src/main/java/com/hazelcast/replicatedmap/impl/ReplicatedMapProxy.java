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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.monitor.impl.EmptyLocalReplicatedMapStats;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.ResultSet;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.operation.ClearOperationFactory;
import com.hazelcast.replicatedmap.impl.operation.PutAllOperation;
import com.hazelcast.replicatedmap.impl.operation.PutOperation;
import com.hazelcast.replicatedmap.impl.operation.RemoveOperation;
import com.hazelcast.replicatedmap.impl.operation.RequestMapDataOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.replicatedmap.impl.record.ReplicatedEntryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.lang.Thread.currentThread;

/**
 * Proxy implementation of {@link ReplicatedMap} interface.
 *
 * @param <K> key type
 * @param <V> value type
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class ReplicatedMapProxy<K, V> extends AbstractDistributedObject<ReplicatedMapService>
        implements ReplicatedMap<K, V>, InitializingObject {

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private static final String NULL_TIMEUNIT_IS_NOT_ALLOWED = "Null time unit is not allowed!";
    private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    private static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Null predicate is not allowed!";

    private static final int WAIT_INTERVAL_MILLIS = 1000;
    private static final int RETRY_INTERVAL_COUNT = 3;
    private static final int KEY_SET_MIN_SIZE = 16;
    private static final int KEY_SET_STORE_MULTIPLE = 4;
    private static final int PARALLEL_INIT_REQUESTS_LIMIT = 100;

    private static final LocalReplicatedMapStats EMPTY_LOCAL_MAP_STATS = new EmptyLocalReplicatedMapStats();

    private final String name;
    private final NodeEngine nodeEngine;
    private final ReplicatedMapService service;
    private final ReplicatedMapEventPublishingService eventPublishingService;
    private final SerializationService serializationService;
    private final InternalPartitionServiceImpl partitionService;
    private final ReplicatedMapConfig config;

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
            syncFill();
        }
    }

    private void syncFill() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        BitSet nonLoadedStores = new BitSet(partitionCount);
        int[] retryCount = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            nonLoadedStores.set(i);
        }
        while (true) {
            int remainingParallelRequests = PARALLEL_INIT_REQUESTS_LIMIT;
            for (int nonLoadedPartition = nonLoadedStores.nextSetBit(0);
                 nonLoadedPartition >= 0 && remainingParallelRequests > 0;
                 nonLoadedPartition = nonLoadedStores.nextSetBit(nonLoadedPartition + 1)) {

                ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, nonLoadedPartition);
                if (store == null || !store.isLoaded()) {
                    if ((retryCount[nonLoadedPartition]++) % RETRY_INTERVAL_COUNT == 0) {
                        requestDataForPartition(nonLoadedPartition);
                        remainingParallelRequests--;
                    }
                } else {
                    nonLoadedStores.clear(nonLoadedPartition);
                }
            }

            if (nonLoadedStores.isEmpty()) {
                break;
            }
            sleep();
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
        ensureNoSplitBrain(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        int size = 0;
        for (ReplicatedRecordStore store : stores) {
            size += store.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        ensureNoSplitBrain(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        for (ReplicatedRecordStore store : stores) {
            if (!store.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(@Nonnull Object key) {
        ensureNoSplitBrain(READ);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        int partitionId = partitionService.getPartitionId(key);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, partitionId);
        return store != null && store.containsKey(key);
    }

    @Override
    public boolean containsValue(@Nonnull Object value) {
        ensureNoSplitBrain(READ);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        for (ReplicatedRecordStore store : stores) {
            if (store.containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(@Nonnull Object key) {
        ensureNoSplitBrain(READ);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        int partitionId = partitionService.getPartitionId(key);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(getName(), false, partitionId);
        if (store == null) {
            return null;
        }
        return (V) store.get(key);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        Data dataKey = nodeEngine.toData(key);
        Data dataValue = nodeEngine.toData(value);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
        PutOperation putOperation = new PutOperation(getName(), dataKey, dataValue);
        InternalCompletableFuture<Object> future = getOperationService()
                .invokeOnPartition(getServiceName(), putOperation, partitionId);
        VersionResponsePair result = (VersionResponsePair) future.joinInternal();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeUnit, NULL_TIMEUNIT_IS_NOT_ALLOWED);
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
        VersionResponsePair result = (VersionResponsePair) future.joinInternal();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public V remove(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data dataKey = nodeEngine.toData(key);
        int partitionId = partitionService.getPartitionId(key);
        RemoveOperation removeOperation = new RemoveOperation(getName(), dataKey);
        InternalCompletableFuture<Object> future = getOperationService()
                .invokeOnPartition(getServiceName(), removeOperation, partitionId);
        VersionResponsePair result = (VersionResponsePair) future.joinInternal();
        return nodeEngine.toObject(result.getResponse());
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> entries) {
        checkNotNull(entries, "Entries cannot be null");
        int mapSize = entries.size();
        if (mapSize == 0) {
            return;
        }

        int partitionCount = partitionService.getPartitionCount();
        int initialSize = getPutAllInitialSize(mapSize, partitionCount);

        try {
            List<Future> futures = new ArrayList<>(partitionCount);
            MapEntries[] entrySetPerPartition = new MapEntries[partitionCount];

            // first we fill entrySetPerPartition
            for (Entry entry : entries.entrySet()) {
                checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
                checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

                int partitionId = partitionService.getPartitionId(entry.getKey());
                MapEntries mapEntries = entrySetPerPartition[partitionId];
                if (mapEntries == null) {
                    mapEntries = new MapEntries(initialSize);
                    entrySetPerPartition[partitionId] = mapEntries;
                }

                Data keyData = serializationService.toData(entry.getKey());
                Data valueData = serializationService.toData(entry.getValue());
                mapEntries.add(keyData, valueData);
            }

            // then we invoke the operations
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                MapEntries entrySet = entrySetPerPartition[partitionId];
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

    private Future createPutAllOperationFuture(String name, MapEntries entrySet, int partitionId) {
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
    public boolean removeEntryListener(@Nonnull UUID id) {
        checkNotNull(id, "Listener ID should not be null!");
        return eventPublishingService.removeEventListener(name, id);
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        return eventPublishingService.addLocalEventListener(listener, TrueEventFilter.INSTANCE, name);
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nullable K key) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        EventFilter eventFilter = new ReplicatedEntryEventFilter(serializationService.toData(key));
        return eventPublishingService.addLocalEventListener(listener, eventFilter, name);
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nonnull Predicate<K, V> predicate) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        EventFilter eventFilter = new ReplicatedQueryEventFilter(null, predicate);
        return eventPublishingService.addLocalEventListener(listener, eventFilter, name);
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener,
                                   @Nonnull Predicate<K, V> predicate,
                                   @Nullable K key) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        EventFilter eventFilter = new ReplicatedQueryEventFilter(serializationService.toData(key), predicate);
        return eventPublishingService.addLocalEventListener(listener, eventFilter, name);
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        ensureNoSplitBrain(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Set<K> keySet = createHashSet(Math.max(KEY_SET_MIN_SIZE, stores.size() * KEY_SET_STORE_MULTIPLE));
        for (ReplicatedRecordStore store : stores) {
            keySet.addAll(store.keySet(true));
        }
        return keySet;
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        ensureNoSplitBrain(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        Collection<V> values = new ArrayList<>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(true));
        }
        return values;
    }

    @Nonnull
    @Override
    public Collection<V> values(@Nullable Comparator<V> comparator) {
        ensureNoSplitBrain(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        List<V> values = new ArrayList<>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(comparator));
        }
        values.sort(comparator);
        return values;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Set<Entry<K, V>> entrySet() {
        ensureNoSplitBrain(READ);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getName());
        List<Entry> entries = new ArrayList<>();
        for (ReplicatedRecordStore store : stores) {
            entries.addAll(store.entrySet(true));
        }
        return (Set) new ResultSet(entries, IterationType.ENTRY);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> " + name;
    }

    @Nonnull
    @Override
    public LocalReplicatedMapStats getReplicatedMapStats() {
        LocalReplicatedMapStats stats;
        if (config.isStatisticsEnabled()) {
            stats = service.getLocalReplicatedMapStats(name);
        } else {
            stats = EMPTY_LOCAL_MAP_STATS;
        }
        return stats;
    }

    private void ensureNoSplitBrain(SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        service.ensureNoSplitBrain(name, requiredSplitBrainProtectionPermissionType);
    }
}

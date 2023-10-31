/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.eviction.ExpirationManager;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.impl.NameSpaceUtil;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.map.impl.operation.MapClearExpiredOperation;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import static com.hazelcast.map.impl.MapKeyLoaderUtil.getMaxSizePerNode;
import static com.hazelcast.map.impl.MapMigrationAwareService.lesserBackupMapsThenWithContainer;

public class PartitionContainer {

    private final int partitionId;
    private final MapService mapService;
    private final ContextMutexFactory contextMutexFactory = new ContextMutexFactory();
    private final ConcurrentMap<String, RecordStore> maps;
    private final ConstructorFunction<String, RecordStore> recordStoreConstructor
            = name -> {
        RecordStore recordStore = createRecordStore(name);
        recordStore.startLoading();
        return recordStore;
    };
    private final ConstructorFunction<String, RecordStore> recordStoreConstructorSkipLoading
            = this::createRecordStore;

    private final ConstructorFunction<String, RecordStore> recordStoreConstructorForHotRestart
            = this::createRecordStore;
    /**
     * Flag to check if there is a {@link MapClearExpiredOperation}
     * running on this partition at this moment or not.
     */
    private volatile boolean hasRunningCleanup;
    private volatile long lastCleanupTime;

    /**
     * Used when sorting partition containers in {@link ExpirationManager}
     * A non-volatile copy of lastCleanupTime is used with two reasons.
     * <p>
     * 1. We need an un-modified field during sorting.
     * 2. Decrease number of volatile reads.
     */
    private long lastCleanupTimeCopy;

    public PartitionContainer(final MapService mapService, final int partitionId) {
        this.mapService = mapService;
        this.partitionId = partitionId;
        int approxMapCount = mapService.mapServiceContext.getNodeEngine()
                .getConfig().getMapConfigs().size();
        this.maps = MapUtil.createConcurrentHashMap(approxMapCount);
    }

    private RecordStore createRecordStore(String name) {
        MapServiceContext serviceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = serviceContext.getMapContainer(name);
        MapConfig mapConfig = mapContainer.getMapConfig();
        NodeEngine nodeEngine = serviceContext.getNodeEngine();
        IPartitionService ps = nodeEngine.getPartitionService();
        OperationService opService = nodeEngine.getOperationService();
        ExecutionService execService = nodeEngine.getExecutionService();
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();

        MapKeyLoader keyLoader = new MapKeyLoader(name, opService, ps, nodeEngine.getClusterService(),
                execService, mapContainer.toData(), serviceContext.getNodeWideLoadedKeyLimiter());
        keyLoader.setMaxBatch(hazelcastProperties.getInteger(ClusterProperty.MAP_LOAD_CHUNK_SIZE));
        keyLoader.setMaxSize(getMaxSizePerNode(mapConfig.getEvictionConfig()));
        keyLoader.setHasBackup(mapConfig.getTotalBackupCount() > 0);
        keyLoader.setMapOperationProvider(serviceContext.getMapOperationProvider(name));
        int partitionId = getPartitionId();

        if (!mapContainer.shouldUseGlobalIndex()) {
            mapContainer.createIndexRegistry(false, partitionId);
        }
        RecordStore recordStore = serviceContext.createRecordStore(mapContainer, partitionId, keyLoader);
        recordStore.init();
        return recordStore;
    }

    public ConcurrentMap<String, RecordStore> getMaps() {
        return maps;
    }

    public Collection<RecordStore> getAllRecordStores() {
        return maps.isEmpty() ? Collections.emptyList() : maps.values();
    }

    public Collection<ServiceNamespace> getAllNamespaces(int replicaIndex) {
        return getNamespaces(ignored -> true, replicaIndex);
    }

    public Collection<ServiceNamespace> getNamespaces(Predicate<MapConfig> predicate, int replicaIndex) {
        return NameSpaceUtil.getAllNamespaces(maps, recordStore -> {
            MapContainer mapContainer = recordStore.getMapContainer();
            MapConfig mapConfig = mapContainer.getMapConfig();
            return mapConfig.getTotalBackupCount() >= replicaIndex && predicate.test(mapConfig);
        }, recordStore -> recordStore.getMapContainer().getObjectNamespace());
    }

    public int getPartitionId() {
        return partitionId;
    }

    public MapService getMapService() {
        return mapService;
    }

    public RecordStore getRecordStore(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name,
                contextMutexFactory, recordStoreConstructor);
    }

    public RecordStore getRecordStore(String name, boolean skipLoadingOnCreate) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, this, skipLoadingOnCreate
                ? recordStoreConstructorSkipLoading : recordStoreConstructor);
    }

    public RecordStore getRecordStoreForHotRestart(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name,
                contextMutexFactory, recordStoreConstructorForHotRestart);
    }

    @Nullable
    public RecordStore getExistingRecordStore(String mapName) {
        return maps.get(mapName);
    }

    public void destroyMap(MapContainer mapContainer) {
        // Mark map container destroyed before the underlying
        // data structures are destroyed. We need this to
        // ensure that every reader that observed non-destroyed
        // state may use previously read data. E.g. if the
        // reader returned only Key1 it is guaranteed that it
        // hadn't missed Key2 because it was destroyed earlier.
        mapContainer.onBeforeDestroy();

        String name = mapContainer.getName();
        RecordStore recordStore = maps.get(name);
        if (recordStore != null) {
            // this call also clears and disposes Indexes for that partition
            recordStore.destroy();
            // Remove record store from the maps after destroy since it could be accessed by
            // mutation observers.
            maps.remove(name);
        } else {
            // It can be that, map is used only for locking,
            // because of that RecordStore is not created.
            // We will try to remove/clear LockStore belonging to
            // this IMap partition.
            clearLockStore(name);
        }

        destroyMapContainer(mapContainer);
        mapService.mapServiceContext.removePartitioningStrategyFromCache(mapContainer.getName());
    }

    /**
     * @return {@code true} if destruction is successful, otherwise
     * return {@code false} if it is already destroyed.
     */
    public boolean destroyMapContainer(MapContainer mapContainer) {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (mapServiceContext.removeMapContainer(mapContainer)) {
            mapContainer.onDestroy();
            return true;
        }
        return false;
    }

    private void clearLockStore(String name) {
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final LockSupportService lockService = nodeEngine.getServiceOrNull(LockSupportService.SERVICE_NAME);
        if (lockService != null) {
            final ObjectNamespace namespace = MapService.getObjectNamespace(name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }

    public boolean hasRunningCleanup() {
        return hasRunningCleanup;
    }

    public void setHasRunningCleanup(boolean hasRunningCleanup) {
        this.hasRunningCleanup = hasRunningCleanup;
    }

    public long getLastCleanupTime() {
        return lastCleanupTime;
    }

    public void setLastCleanupTime(long lastCleanupTime) {
        this.lastCleanupTime = lastCleanupTime;
    }

    public long getLastCleanupTimeCopy() {
        return lastCleanupTimeCopy;
    }

    public void setLastCleanupTimeCopy(long lastCleanupTimeCopy) {
        this.lastCleanupTimeCopy = lastCleanupTimeCopy;
    }

    /**
     * Cleans up the container's state if the enclosing partition is migrated
     * off this member. Whether cleanup is needed is decided based on the
     * provided {@code replicaIndex}.
     *
     * @param replicaIndex The replica index to use for deciding per map whether
     *                     cleanup is necessary or not
     */
    final void cleanUpOnMigration(int replicaIndex) {
        mapService.getMapServiceContext().getMapContainers().entrySet()
                .stream()
                .filter(entry -> replicaIndex == -1
                        || lesserBackupMapsThenWithContainer(replicaIndex).test(entry.getValue()))
                .forEach(entry -> cleanUpMap(entry.getKey()));
    }

    protected void cleanUpMap(String mapName) {
        // overridden in enterprise
    }
}

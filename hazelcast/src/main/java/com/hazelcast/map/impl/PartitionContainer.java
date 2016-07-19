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

package com.hazelcast.map.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.MapKeyLoaderUtil.getMaxSizePerNode;

public class PartitionContainer {

    final MapService mapService;
    final int partitionId;
    final ConcurrentMap<String, RecordStore> maps = new ConcurrentHashMap<String, RecordStore>(1000);
    final ConstructorFunction<String, RecordStore> recordStoreConstructor
            = new ConstructorFunction<String, RecordStore>() {

        @Override
        public RecordStore createNew(String name) {
            RecordStore recordStore = createRecordStore(name);
            recordStore.startLoading();
            return recordStore;
        }
    };

    final ConstructorFunction<String, RecordStore> recordStoreConstructorSkipLoading
            = new ConstructorFunction<String, RecordStore>() {

        @Override
        public RecordStore createNew(String name) {
            return createRecordStore(name);
        }
    };

    final ConstructorFunction<String, RecordStore> recordStoreConstructorForHotRestart
            = new ConstructorFunction<String, RecordStore>() {

        @Override
        public RecordStore createNew(String name) {
            return createRecordStore(name);
        }
    };
    /**
     * Flag to check if there is a {@link com.hazelcast.map.impl.operation.ClearExpiredOperation}
     * is running on this partition at this moment or not.
     */
    volatile boolean hasRunningCleanup;

    volatile long lastCleanupTime;

    /**
     * Used when sorting partition containers in {@link com.hazelcast.map.impl.eviction.ExpirationManager}
     * A non-volatile copy of lastCleanupTime is used with two reasons.
     * <p/>
     * 1. We need an un-modified field during sorting.
     * 2. Decrease number of volatile reads.
     */
    long lastCleanupTimeCopy;

    private final ContextMutexFactory contextMutexFactory = new ContextMutexFactory();

    public PartitionContainer(final MapService mapService, final int partitionId) {
        this.mapService = mapService;
        this.partitionId = partitionId;
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
                execService, mapContainer.toData());
        keyLoader.setMaxBatch(hazelcastProperties.getInteger(GroupProperty.MAP_LOAD_CHUNK_SIZE));
        keyLoader.setMaxSize(getMaxSizePerNode(mapConfig.getMaxSizeConfig()));
        keyLoader.setHasBackup(mapConfig.getTotalBackupCount() > 0);
        keyLoader.setMapOperationProvider(serviceContext.getMapOperationProvider(name));
        RecordStore recordStore = serviceContext.createRecordStore(mapContainer, partitionId, keyLoader);
        recordStore.init();
        return recordStore;
    }

    public ConcurrentMap<String, RecordStore> getMaps() {
        return maps;
    }

    public Collection<RecordStore> getAllRecordStores() {
        return maps.values();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public MapService getMapService() {
        return mapService;
    }

    public RecordStore getRecordStore(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, contextMutexFactory, recordStoreConstructor);
    }

    public RecordStore getRecordStore(String name, boolean skipLoadingOnCreate) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, this, skipLoadingOnCreate
                ? recordStoreConstructorSkipLoading : recordStoreConstructor);
    }

    public RecordStore getRecordStoreForHotRestart(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, contextMutexFactory, recordStoreConstructorForHotRestart);
    }

    public RecordStore getExistingRecordStore(String mapName) {
        return maps.get(mapName);
    }

    public void destroyMap(MapContainer mapContainer) {
        String name = mapContainer.getName();
        RecordStore recordStore = maps.remove(name);
        if (recordStore != null) {
            recordStore.destroy();
        } else {
            // It can be that, map is used only for locking,
            // because of that RecordStore is not created.
            // We will try to remove/clear LockStore belonging to
            // this IMap partition.
            clearLockStore(name);
        }

        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (mapServiceContext.removeMapContainer(mapContainer)) {
            mapContainer.onDestroy();
        }
        PartitioningStrategyFactory.removePartitioningStrategyFromCache(mapContainer.getName());
    }

    private void clearLockStore(String name) {
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            final DefaultObjectNamespace namespace = new DefaultObjectNamespace(MapService.SERVICE_NAME, name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }

    public void clear(boolean onShutdown) {
        for (RecordStore recordStore : maps.values()) {
            recordStore.clearPartition(onShutdown);
        }
        maps.clear();
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

}

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

package com.hazelcast.map.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.config.MapConfig;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.MapKeyLoaderUtil.getMaxSizePerNode;

public class PartitionContainer {

    private final MapService mapService;

    private final int partitionId;

    private final ConcurrentMap<String, RecordStore> maps = new ConcurrentHashMap<String, RecordStore>(1000);

    private final ConstructorFunction<String, RecordStore> recordStoreConstructor
            = new ConstructorFunction<String, RecordStore>() {

        public RecordStore createNew(String name) {
            MapServiceContext serviceContext = mapService.getMapServiceContext();
            MapContainer mapContainer = serviceContext.getMapContainer(name);
            MapConfig mapConfig = mapContainer.getMapConfig();
            NodeEngine nodeEngine = serviceContext.getNodeEngine();
            InternalPartitionService ps = nodeEngine.getPartitionService();
            OperationService opService = nodeEngine.getOperationService();
            ExecutionService execService = nodeEngine.getExecutionService();
            GroupProperties groupProperties = nodeEngine.getGroupProperties();

            MapKeyLoader keyLoader = new MapKeyLoader(name, opService, ps, execService, mapContainer.toData());
            keyLoader.setMaxBatch(groupProperties.MAP_LOAD_CHUNK_SIZE.getInteger());
            keyLoader.setMaxSize(getMaxSizePerNode(mapConfig.getMaxSizeConfig()));
            keyLoader.setHasBackup(mapConfig.getBackupCount() > 0 || mapConfig.getAsyncBackupCount() > 0);

            ILogger logger = nodeEngine.getLogger(DefaultRecordStore.class);
            DefaultRecordStore recordStore = new DefaultRecordStore(mapContainer, partitionId, keyLoader, logger);
            recordStore.startLoading();

            return recordStore;
        }
    };

    /**
     * Flag to check if there is a {@link com.hazelcast.map.impl.operation.ClearExpiredOperation}
     * is running on this partition at this moment or not.
     */
    private volatile boolean hasRunningCleanup;

    private volatile long lastCleanupTime;

    /**
     * Used when sorting partition containers in {@link com.hazelcast.map.impl.eviction.ExpirationManager}
     * A non-volatile copy of lastCleanupTime is used with two reasons.
     * <p>
     * 1. We need an un-modified field during sorting.
     * 2. Decrease number of volatile reads.
     */
    private long lastCleanupTimeCopy;

    public PartitionContainer(final MapService mapService, final int partitionId) {
        this.mapService = mapService;
        this.partitionId = partitionId;
    }

    public ConcurrentMap<String, RecordStore> getMaps() {
        return maps;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public MapService getMapService() {
        return mapService;
    }

    public RecordStore getRecordStore(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, this, recordStoreConstructor);
    }

    public RecordStore getExistingRecordStore(String mapName) {
        return maps.get(mapName);
    }

    public void destroyMap(String name) {
        RecordStore recordStore = maps.remove(name);
        if (recordStore != null) {
            recordStore.clearPartition();
        } else {
            // It can be that, map is used only for locking,
            // because of that RecordStore is not created.
            // We will try to remove/clear LockStore belonging to
            // this IMap partition.
            clearLockStore(name);
        }
    }

    private void clearLockStore(String name) {
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            final DefaultObjectNamespace namespace = new DefaultObjectNamespace(MapService.SERVICE_NAME, name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }

    void clear() {
        for (RecordStore recordStore : maps.values()) {
            recordStore.clearPartition();
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

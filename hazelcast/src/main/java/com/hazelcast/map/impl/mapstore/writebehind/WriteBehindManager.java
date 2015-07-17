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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapDataStores;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.MapStoreManager;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.ExecutorType;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindProcessors.createWriteBehindProcessor;

/**
 * Write behind map store manager.
 */
public class WriteBehindManager implements MapStoreManager {

    private static final String EXECUTOR_NAME_PREFIX = "hz:scheduled:mapstore:";

    private static final int EXECUTOR_DEFAULT_QUEUE_CAPACITY = 10000;

    private final ScheduledExecutorService scheduledExecutor;

    private final WriteBehindProcessor writeBehindProcessor;

    private final StoreWorker storeWorker;

    private final String executorName;

    private final MapStoreContext mapStoreContext;

    public WriteBehindManager(MapStoreContext mapStoreContext) {
        this.mapStoreContext = mapStoreContext;
        this.executorName = EXECUTOR_NAME_PREFIX + mapStoreContext.getMapName();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        this.scheduledExecutor = getScheduledExecutorService(mapServiceContext);
        this.writeBehindProcessor = newWriteBehindProcessor(mapStoreContext, scheduledExecutor);
        this.storeWorker = new StoreWorker(mapStoreContext, writeBehindProcessor);
    }

    public void start() {
        scheduledExecutor.scheduleAtFixedRate(storeWorker, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        nodeEngine.getExecutionService().shutdownExecutor(executorName);
    }

    //todo get this via constructor function.
    @Override
    public MapDataStore getMapDataStore(int partitionId) {
        return MapDataStores.createWriteBehindStore(mapStoreContext, partitionId, writeBehindProcessor);
    }

    private WriteBehindProcessor newWriteBehindProcessor(final MapStoreContext mapStoreContext,
                                                         ScheduledExecutorService scheduledExecutor) {
        WriteBehindProcessor writeBehindProcessor = createWriteBehindProcessor(mapStoreContext);
        final WriteBehindBackupPartitionCleaner writeBehindBackupPartitionCleaner
                = new WriteBehindBackupPartitionCleaner(mapStoreContext);
        StoreListener<DelayedEntry> storeListener
                = new InternalStoreListener(mapStoreContext, writeBehindBackupPartitionCleaner);
        writeBehindProcessor.addStoreListener(storeListener);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                writeBehindBackupPartitionCleaner.removeFromBackups();
            }
        };
        scheduledExecutor.scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
        return writeBehindProcessor;
    }

    private ScheduledExecutorService getScheduledExecutorService(MapServiceContext mapServiceContext) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        return executionService.getScheduledExecutor(executorName);
    }

    /**
     * Store listener which is responsible for
     * {@link com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore#stagingArea cleaning.
     * <p/>
     * Called from at most one thread at a time.
     */
    private class InternalStoreListener implements StoreListener<DelayedEntry> {

        private final WriteBehindBackupPartitionCleaner writeBehindBackupPartitionCleaner;
        private final MapStoreContext mapStoreContext;

        public InternalStoreListener(MapStoreContext mapStoreContext,
                                     WriteBehindBackupPartitionCleaner writeBehindBackupPartitionCleaner) {
            this.mapStoreContext = mapStoreContext;
            this.writeBehindBackupPartitionCleaner = writeBehindBackupPartitionCleaner;
        }

        @Override
        public void beforeStore(StoreEvent<DelayedEntry> storeEvent) {

        }

        /**
         * Here we are cleaning staging area upon a store operation.
         */
        @Override
        public void afterStore(StoreEvent<DelayedEntry> storeEvent) {
            DelayedEntry delayedEntry = storeEvent.getSource();
            int partitionId = delayedEntry.getPartitionId();
            WriteBehindStore writeBehindStore = getWriteBehindStoreOrNull(partitionId);
            if (writeBehindStore == null) {
                return;
            }

            writeBehindStore.removeFromStagingArea(delayedEntry);
            writeBehindBackupPartitionCleaner.add(partitionId);
        }

        private WriteBehindStore getWriteBehindStoreOrNull(int partitionId) {
            MapStoreContext mapStoreContext = this.mapStoreContext;
            MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapStoreContext.getMapName());
            if (recordStore == null) {
                return null;
            }
            return (WriteBehindStore) recordStore.getMapDataStore();
        }

    }
}

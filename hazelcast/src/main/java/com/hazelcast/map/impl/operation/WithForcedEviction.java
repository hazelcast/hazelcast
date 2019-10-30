/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * Includes retry logic when a map operation fails to put an entry
 * into {@code IMap} due to a {@link NativeOutOfMemoryError}.
 * <p>
 * If an {@code IMap} is evictable, naturally expected thing is, all put
 * operations should be successful. Because if there is no more space,
 * operation can be able to evict some entries and can put the new ones.
 * <p>
 * This abstract class forces the evictable record-stores on this partition
 * thread to be evicted in the event of a {@link NativeOutOfMemoryError}.
 * <p>
 * Used when {@link com.hazelcast.config.InMemoryFormat InMemoryFormat}
 * is {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE}.
 */
public final class WithForcedEviction {

    public static final int DEFAULT_FORCED_EVICTION_RETRY_COUNT = 5;
    public static final String PROP_FORCED_EVICTION_RETRY_COUNT
            = "hazelcast.internal.forced.eviction.retry.count";
    public static final HazelcastProperty FORCED_EVICTION_RETRY_COUNT
            = new HazelcastProperty(PROP_FORCED_EVICTION_RETRY_COUNT,
            DEFAULT_FORCED_EVICTION_RETRY_COUNT);

    private WithForcedEviction() {
    }

    static void rerun(MapOperation mapOperation) {
        try {
            rerunWithForcedEviction(mapOperation);
        } catch (NativeOutOfMemoryError e) {
            mapOperation.disposeDeferredBlocks();
            throw e;
        }
    }

    private static void rerunWithForcedEviction(MapOperation mapOperation) {
        ILogger logger = mapOperation.logger();
        int forcedEvictionRetryCount = getRetryCount(mapOperation);

        // first attempt failed, so lets try to evict
        // the current record store and then try again
        for (int i = 0; i < forcedEvictionRetryCount; i++) {
            try {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Applying forced eviction on current RecordStore (map %s, partitionId: %d)!",
                            mapOperation.getName(), mapOperation.getPartitionId()));
                }
                // if there is still an NOOME, apply eviction on current RecordStore and try again
                forceEviction(mapOperation.recordStore);
                mapOperation.runInternal();
                return;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }

        // cleaning up the current record stores didn't help, so
        // lets try to clean the other record stores and try again
        for (int i = 0; i < forcedEvictionRetryCount; i++) {
            try {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Applying forced eviction on other RecordStores owned by the same partition thread"
                            + " (map %s, partitionId: %d", mapOperation.getName(), mapOperation.getPartitionId()));
                }
                // if there is still an NOOME, apply for eviction on others and try again
                forceEvictionOnOthers(mapOperation);
                mapOperation.runInternal();
                return;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }

        evictAllAndRetry(logger, mapOperation);
    }

    private static void evictAllAndRetry(ILogger logger, MapOperation mapOperation) {
        boolean isBackup = mapOperation instanceof BackupOperation;

        RecordStore recordStore = mapOperation.recordStore;
        if (recordStore != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Evicting all entries in current"
                            + " RecordStores because forced eviction was not enough!");
                }
                // if there is still NOOME, clear the current RecordStore and try again
                evictAllThenDispose(recordStore, isBackup);
                mapOperation.runInternal();
                return;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Evicting all entries in other RecordStores owned by the same partition thread"
                    + " because forced eviction was not enough!");
        }
        // if there is still NOOME, for the last chance, evict other record stores and try again
        evictAll(mapOperation, isBackup);
        mapOperation.runInternal();
    }

    /**
     * Executes a forced eviction on this particular RecordStore.
     */
    private static void forceEviction(RecordStore recordStore) {
        if (recordStore == null) {
            return;
        }

        MapContainer mapContainer = recordStore.getMapContainer();
        MapConfig mapConfig = mapContainer.getMapConfig();
        if (mapConfig.getInMemoryFormat() == NATIVE
                && mapConfig.getEvictionConfig().getEvictionPolicy() != NONE) {
            Evictor evictor = mapContainer.getEvictor();
            evictor.forceEvict(recordStore);
        }
    }

    /**
     * Executes a forced eviction on other NATIVE
     * in-memory-formatted RecordStores of this partition thread.
     */
    private static void forceEvictionOnOthers(MapOperation mapOperation) {
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        OperationService operationService = nodeEngine.getOperationService();
        int threadCount = operationService.getPartitionThreadCount();
        int mod = mapOperation.getPartitionId() % threadCount;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ConcurrentMap<String, RecordStore> maps = getRecordStoresInThisPartition(mapOperation, partitionId);
                for (RecordStore recordStore : maps.values()) {
                    forceEviction(recordStore);
                }
            }
        }
    }

    /**
     * Evicts all RecordStores on the partitions owned
     * by the partition thread of current partition.
     */
    private static void evictAll(MapOperation mapOperation, boolean isBackup) {
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        OperationService operationService = nodeEngine.getOperationService();
        int threadCount = operationService.getPartitionThreadCount();
        int mod = mapOperation.getPartitionId() % threadCount;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ConcurrentMap<String, RecordStore> maps = getRecordStoresInThisPartition(mapOperation, partitionId);
                for (RecordStore recordStore : maps.values()) {
                    MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
                    if (mapConfig.getInMemoryFormat() == NATIVE
                            && mapConfig.getEvictionConfig().getEvictionPolicy() != NONE) {
                        evictAllThenDispose(recordStore, isBackup);
                    }
                }
            }
        }
    }

    private static ConcurrentMap<String, RecordStore> getRecordStoresInThisPartition(MapOperation mapOperation,
                                                                                     int partitionId) {
        MapServiceContext mapServiceContext = mapOperation.mapServiceContext;
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        return partitionContainer.getMaps();
    }

    private static void evictAllThenDispose(RecordStore recordStore, boolean isBackup) {
        recordStore.evictAll(isBackup);
        recordStore.disposeDeferredBlocks();
    }

    private static int getRetryCount(Operation operation) {
        HazelcastProperties properties = operation.getNodeEngine().getProperties();
        return properties.getInteger(FORCED_EVICTION_RETRY_COUNT);
    }
}

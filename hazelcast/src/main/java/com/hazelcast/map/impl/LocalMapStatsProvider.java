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

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Provides node local statistics of a map via {@link #createLocalMapStats}
 * and also holds all {@link com.hazelcast.monitor.impl.LocalMapStatsImpl} implementations of all maps.
 */
public class LocalMapStatsProvider {

    protected static final int WAIT_PARTITION_TABLE_UPDATE_MILLIS = 100;
    protected static final int RETRY_COUNT = 3;

    protected final ConcurrentMap<String, LocalMapStatsImpl> statsMap
            = new ConcurrentHashMap<String, LocalMapStatsImpl>(1000);
    protected final ConstructorFunction<String, LocalMapStatsImpl> constructorFunction
            = new ConstructorFunction<String, LocalMapStatsImpl>() {
        public LocalMapStatsImpl createNew(String key) {
            return new LocalMapStatsImpl();
        }
    };

    protected final MapServiceContext mapServiceContext;
    protected final NearCacheProvider nearCacheProvider;
    protected final ClusterService clusterService;
    protected final IPartitionService partitionService;
    protected final ILogger logger;

    public LocalMapStatsProvider(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.nearCacheProvider = mapServiceContext.getNearCacheProvider();
        this.clusterService = nodeEngine.getClusterService();
        this.partitionService = nodeEngine.getPartitionService();
    }

    public LocalMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public void destroyLocalMapStatsImpl(String name) {
        statsMap.remove(name);
    }

    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        LocalMapStatsImpl stats = getLocalMapStatsImpl(mapName);
        if (!mapContainer.getMapConfig().isStatisticsEnabled()) {
            return stats;
        }
        int backupCount = mapContainer.getTotalBackupCount();
        Address thisAddress = clusterService.getThisAddress();

        LocalMapOnDemandCalculatedStats onDemandStats = new LocalMapOnDemandCalculatedStats();
        onDemandStats.setBackupCount(backupCount);

        addNearCacheStats(stats, onDemandStats, mapContainer);

        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            IPartition partition = partitionService.getPartition(partitionId);
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                //no-op because no owner is set yet. Therefore we don't know anything about the map
                continue;
            }

            if (owner.equals(thisAddress)) {
                addOwnerPartitionStats(stats, onDemandStats, mapName, partitionId);
            } else {
                addReplicaPartitionStats(onDemandStats, mapName, partitionId,
                        partition, partitionService, backupCount, thisAddress);
            }
        }

        onDemandStats.copyValuesTo(stats);

        return stats;
    }

    /**
     * Calculates and adds owner partition stats.
     */
    protected void addOwnerPartitionStats(LocalMapStatsImpl stats,
                                          LocalMapOnDemandCalculatedStats onDemandStats,
                                          String mapName, int partitionId) {
        RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
        if (!hasRecords(recordStore)) {
            return;
        }

        onDemandStats.incrementLockedEntryCount(recordStore.getLockedEntryCount());
        onDemandStats.incrementHits(recordStore.getHits());
        onDemandStats.incrementDirtyEntryCount(recordStore.getMapDataStore().notFinishedOperationsCount());
        onDemandStats.incrementOwnedEntryMemoryCost(recordStore.getHeapCost());
        onDemandStats.incrementHeapCost(recordStore.getHeapCost());
        onDemandStats.incrementOwnedEntryCount(recordStore.size());

        stats.setLastAccessTime(recordStore.getLastAccessTime());
        stats.setLastUpdateTime(recordStore.getLastUpdateTime());
    }

    /**
     * Return 1 if locked, otherwise 0.
     * Used to find {@link LocalMapStatsImpl#lockedEntryCount}.
     */
    protected int isLocked(Data key, RecordStore recordStore) {
        if (recordStore.isLocked(key)) {
            return 1;
        }
        return 0;
    }

    /**
     * Calculates and adds replica partition stats.
     */
    protected void addReplicaPartitionStats(LocalMapOnDemandCalculatedStats onDemandStats,
                                            String mapName, int partitionId, IPartition partition,
                                            IPartitionService partitionService, int backupCount, Address thisAddress) {
        long backupEntryCount = 0;
        long backupEntryMemoryCost = 0;

        for (int replica = 1; replica <= backupCount; replica++) {
            Address replicaAddress = getReplicaAddress(replica, partition, partitionService, backupCount);
            if (!isReplicaAvailable(replicaAddress, partitionService, backupCount)) {
                printWarning(partition, replica);
                continue;
            }
            if (isReplicaOnThisNode(replicaAddress, thisAddress)) {
                RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
                if (hasRecords(recordStore)) {
                    backupEntryMemoryCost += recordStore.getHeapCost();
                    backupEntryCount += recordStore.size();
                }
            }
        }
        onDemandStats.incrementHeapCost(backupEntryMemoryCost);
        onDemandStats.incrementBackupEntryMemoryCost(backupEntryMemoryCost);
        onDemandStats.incrementBackupEntryCount(backupEntryCount);
    }

    protected boolean hasRecords(RecordStore recordStore) {
        return recordStore != null && recordStore.size() > 0;
    }

    protected boolean isReplicaAvailable(Address replicaAddress, IPartitionService partitionService, int backupCount) {
        return !(replicaAddress == null && partitionService.getMaxAllowedBackupCount() >= backupCount);
    }

    protected boolean isReplicaOnThisNode(Address replicaAddress, Address thisAddress) {
        return replicaAddress != null && replicaAddress.equals(thisAddress);
    }

    protected void printWarning(IPartition partition, int replica) {
        logger.warning("Partition: " + partition + ", replica: " + replica + " has no owner!");
    }


    protected RecordStore getRecordStoreOrNull(String mapName, int partitionId) {
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        return partitionContainer.getExistingRecordStore(mapName);
    }

    /**
     * Gets replica address. Waits if necessary.
     *
     * @see #waitForReplicaAddress
     */
    protected Address getReplicaAddress(int replica, IPartition partition, IPartitionService partitionService,
                                        int backupCount) {
        Address replicaAddress = partition.getReplicaAddress(replica);
        if (replicaAddress == null) {
            replicaAddress = waitForReplicaAddress(replica, partition, partitionService, backupCount);
        }
        return replicaAddress;
    }

    /**
     * Waits partition table update to get replica address if current replica address is null.
     */
    protected Address waitForReplicaAddress(int replica, IPartition partition, IPartitionService partitionService,
                                            int backupCount) {
        int tryCount = RETRY_COUNT;
        Address replicaAddress = null;
        while (replicaAddress == null && partitionService.getMaxAllowedBackupCount() >= backupCount && tryCount-- > 0) {
            sleep();
            replicaAddress = partition.getReplicaAddress(replica);
        }
        return replicaAddress;
    }

    protected void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_PARTITION_TABLE_UPDATE_MILLIS);
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    /**
     * Adds near cache stats.
     */
    protected void addNearCacheStats(LocalMapStatsImpl stats,
                                     LocalMapOnDemandCalculatedStats onDemandStats, MapContainer mapContainer) {
        if (!mapContainer.getMapConfig().isNearCacheEnabled()) {
            return;
        }
        NearCache nearCache = nearCacheProvider.getOrCreateNearCache(mapContainer.getName());
        NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
        long nearCacheHeapCost = mapContainer.getNearCacheSizeEstimator().getSize();

        stats.setNearCacheStats(nearCacheStats);
        onDemandStats.incrementHeapCost(nearCacheHeapCost);
    }

    protected static class LocalMapOnDemandCalculatedStats {

        protected long hits;

        protected long ownedEntryCount;
        protected long backupEntryCount;
        protected long ownedEntryMemoryCost;
        protected long backupEntryMemoryCost;
        /**
         * Holds total heap cost of map & near-cache & backups.
         */
        protected long heapCost;
        protected long lockedEntryCount;
        protected long dirtyEntryCount;
        protected int backupCount;

        public void setBackupCount(int backupCount) {
            this.backupCount = backupCount;
        }

        public void incrementHits(long hits) {
            this.hits += hits;
        }

        public void incrementOwnedEntryCount(long ownedEntryCount) {
            this.ownedEntryCount += ownedEntryCount;
        }

        public void incrementBackupEntryCount(long backupEntryCount) {
            this.backupEntryCount += backupEntryCount;
        }

        public void incrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
            this.ownedEntryMemoryCost += ownedEntryMemoryCost;
        }

        public void incrementBackupEntryMemoryCost(long backupEntryMemoryCost) {
            this.backupEntryMemoryCost += backupEntryMemoryCost;
        }

        public void incrementLockedEntryCount(long lockedEntryCount) {
            this.lockedEntryCount += lockedEntryCount;
        }

        public void incrementDirtyEntryCount(long dirtyEntryCount) {
            this.dirtyEntryCount += dirtyEntryCount;
        }

        public void incrementHeapCost(long heapCost) {
            this.heapCost += heapCost;
        }

        public void copyValuesTo(LocalMapStatsImpl stats) {
            stats.setBackupCount(backupCount);
            stats.setHits(hits);
            stats.setOwnedEntryCount(ownedEntryCount);
            stats.setBackupEntryCount(backupEntryCount);
            stats.setOwnedEntryMemoryCost(ownedEntryMemoryCost);
            stats.setBackupEntryMemoryCost(backupEntryMemoryCost);
            stats.setHeapCost(heapCost);
            stats.setLockedEntryCount(lockedEntryCount);
            stats.setDirtyEntryCount(dirtyEntryCount);
        }

    }

}

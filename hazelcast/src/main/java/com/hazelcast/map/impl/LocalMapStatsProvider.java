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

package com.hazelcast.map.impl;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalRecordStoreStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.IndexesStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.OnDemandIndexStats;
import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.Address;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.proxyservice.ProxyService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Provides node local statistics of a map via {@link #createLocalMapStats}
 * and also holds all {@link com.hazelcast.monitor.impl.LocalMapStatsImpl} implementations of all maps.
 */
public class LocalMapStatsProvider {

    public static final LocalMapStats EMPTY_LOCAL_MAP_STATS = new LocalMapStatsImpl();

    private static final int RETRY_COUNT = 3;
    private static final int WAIT_PARTITION_TABLE_UPDATE_MILLIS = 100;

    private final ILogger logger;
    private final Address localAddress;
    private final NodeEngine nodeEngine;
    private final ClusterService clusterService;
    private final MapServiceContext mapServiceContext;
    private final MapNearCacheManager mapNearCacheManager;
    private final IPartitionService partitionService;
    private final ConcurrentMap<String, LocalMapStatsImpl> statsMap = new ConcurrentHashMap<>(1000);
    private final ConstructorFunction<String, LocalMapStatsImpl> constructorFunction =
            key -> new LocalMapStatsImpl();

    public LocalMapStatsProvider(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        this.clusterService = nodeEngine.getClusterService();
        this.partitionService = nodeEngine.getPartitionService();
        this.localAddress = clusterService.getThisAddress();
    }

    protected MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    public LocalMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public void destroyLocalMapStatsImpl(String name) {
        statsMap.remove(name);
    }

    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        LocalMapStatsImpl stats = getLocalMapStatsImpl(mapName);
        LocalMapOnDemandCalculatedStats onDemandStats = new LocalMapOnDemandCalculatedStats();
        addNearCacheStats(mapName, stats, onDemandStats);
        addIndexStats(mapName, stats);
        updateMapOnDemandStats(mapName, onDemandStats);

        return onDemandStats.updateAndGet(stats);
    }

    public Map<String, LocalMapStats> createAllLocalMapStats() {
        Map statsPerMap = new HashMap();

        PartitionContainer[] partitionContainers = mapServiceContext.getPartitionContainers();
        for (PartitionContainer partitionContainer : partitionContainers) {
            Collection<RecordStore> allRecordStores = partitionContainer.getAllRecordStores();
            for (RecordStore recordStore : allRecordStores) {
                if (!isStatsCalculationEnabledFor(recordStore)) {
                    continue;
                }
                IPartition partition = partitionService.getPartition(partitionContainer.getPartitionId(), false);
                if (partition.isLocal()) {
                    addPrimaryStatsOf(recordStore, getOrCreateOnDemandStats(statsPerMap, recordStore));
                } else {
                    addReplicaStatsOf(recordStore, getOrCreateOnDemandStats(statsPerMap, recordStore));
                }
            }
        }

        // reuse same HashMap to return calculated LocalMapStats.
        for (Object object : statsPerMap.entrySet()) {
            Map.Entry entry = (Map.Entry) object;
            String mapName = ((String) entry.getKey());
            LocalMapStatsImpl existingStats = getLocalMapStatsImpl(mapName);
            LocalMapOnDemandCalculatedStats onDemand = ((LocalMapOnDemandCalculatedStats) entry.getValue());
            addNearCacheStats(mapName, existingStats, onDemand);
            addIndexStats(mapName, existingStats);
            addStructureStats(mapName, onDemand);

            LocalMapStatsImpl updatedStats = onDemand.updateAndGet(existingStats);
            entry.setValue(updatedStats);
        }

        addStatsOfNoDataIncludedMaps(statsPerMap);

        return statsPerMap;
    }

    /**
     * Some maps may have a proxy but no data has been put yet. Think of one created a proxy but not put any data in it.
     * By calling this method we are returning an empty stats object for those maps. This is helpful to monitor those kind
     * of maps.
     */
    private void addStatsOfNoDataIncludedMaps(Map statsPerMap) {
        ProxyService proxyService = nodeEngine.getProxyService();
        Collection<String> mapNames = proxyService.getDistributedObjectNames(SERVICE_NAME);
        for (String mapName : mapNames) {
            if (!statsPerMap.containsKey(mapName)) {
                statsPerMap.put(mapName, EMPTY_LOCAL_MAP_STATS);
            }
        }
    }

    private static boolean isStatsCalculationEnabledFor(RecordStore recordStore) {
        return recordStore.getMapContainer().getMapConfig().isStatisticsEnabled();
    }

    private static LocalMapOnDemandCalculatedStats getOrCreateOnDemandStats(Map<String, Object> onDemandStats,
                                                                            RecordStore recordStore) {
        String mapName = recordStore.getName();

        Object stats = onDemandStats.get(mapName);
        if (stats == null) {
            stats = new LocalMapOnDemandCalculatedStats();
            onDemandStats.put(mapName, stats);
        }
        return ((LocalMapOnDemandCalculatedStats) stats);
    }

    private void updateMapOnDemandStats(String mapName, LocalMapOnDemandCalculatedStats onDemandStats) {
        PartitionContainer[] partitionContainers = mapServiceContext.getPartitionContainers();
        for (PartitionContainer partitionContainer : partitionContainers) {
            IPartition partition = partitionService.getPartition(partitionContainer.getPartitionId());

            if (partition.isLocal()) {
                addPrimaryStatsOf(partitionContainer.getExistingRecordStore(mapName), onDemandStats);
            } else {
                addReplicaStatsOf(partitionContainer.getExistingRecordStore(mapName), onDemandStats);
            }
        }
        addStructureStats(mapName, onDemandStats);
    }

    /**
     * Adds stats related to the data structure itself that should be
     * reported even if the map or some of its {@link RecordStore} is empty
     *
     * @param mapName       The name of the map
     * @param onDemandStats The on-demand map statistics
     */
    protected void addStructureStats(String mapName, LocalMapOnDemandCalculatedStats onDemandStats) {
        // NOP
    }

    private static void addPrimaryStatsOf(RecordStore recordStore, LocalMapOnDemandCalculatedStats onDemandStats) {
        if (recordStore != null) {
            // we need to update the locked entry count here whether or not the map is empty
            // keys that are not contained by a map can be locked
            onDemandStats.incrementLockedEntryCount(recordStore.getLockedEntryCount());
        }

        if (!hasRecords(recordStore)) {
            return;
        }

        LocalRecordStoreStats stats = recordStore.getLocalRecordStoreStats();

        onDemandStats.incrementHits(stats.getHits());
        onDemandStats.incrementDirtyEntryCount(recordStore.getMapDataStore().notFinishedOperationsCount());
        onDemandStats.incrementOwnedEntryMemoryCost(recordStore.getOwnedEntryCost());
        if (NATIVE != recordStore.getMapContainer().getMapConfig().getInMemoryFormat()) {
            onDemandStats.incrementHeapCost(recordStore.getOwnedEntryCost());
        }
        onDemandStats.incrementOwnedEntryCount(recordStore.size());
        onDemandStats.setLastAccessTime(stats.getLastAccessTime());
        onDemandStats.setLastUpdateTime(stats.getLastUpdateTime());
        onDemandStats.setBackupCount(recordStore.getMapContainer().getMapConfig().getTotalBackupCount());
    }

    /**
     * Calculates and adds replica partition stats.
     */
    private void addReplicaStatsOf(RecordStore recordStore, LocalMapOnDemandCalculatedStats onDemandStats) {
        if (!hasRecords(recordStore)) {
            return;
        }

        long backupEntryCount = 0;
        long backupEntryMemoryCost = 0;

        int totalBackupCount = recordStore.getMapContainer().getTotalBackupCount();
        for (int replicaNumber = 1; replicaNumber <= totalBackupCount; replicaNumber++) {
            int partitionId = recordStore.getPartitionId();
            Address replicaAddress = getReplicaAddress(partitionId, replicaNumber, totalBackupCount);
            if (!isReplicaAvailable(replicaAddress, totalBackupCount)) {
                printWarning(partitionId, replicaNumber);
                continue;
            }
            if (isReplicaOnThisNode(replicaAddress)) {
                backupEntryMemoryCost += recordStore.getOwnedEntryCost();
                backupEntryCount += recordStore.size();
            }
        }

        if (NATIVE != recordStore.getMapContainer().getMapConfig().getInMemoryFormat()) {
            onDemandStats.incrementHeapCost(backupEntryMemoryCost);
        }
        onDemandStats.incrementBackupEntryMemoryCost(backupEntryMemoryCost);
        onDemandStats.incrementBackupEntryCount(backupEntryCount);
        onDemandStats.setBackupCount(recordStore.getMapContainer().getMapConfig().getTotalBackupCount());
    }

    private static boolean hasRecords(RecordStore recordStore) {
        return recordStore != null && recordStore.size() > 0;
    }

    private boolean isReplicaAvailable(Address replicaAddress, int backupCount) {
        return !(replicaAddress == null && partitionService.getMaxAllowedBackupCount() >= backupCount);
    }

    private boolean isReplicaOnThisNode(Address replicaAddress) {
        return replicaAddress != null && localAddress.equals(replicaAddress);
    }

    private void printWarning(int partitionId, int replica) {
        logger.warning("partitionId: " + partitionId + ", replica: " + replica + " has no owner!");
    }

    /**
     * Gets replica address. Waits if necessary.
     *
     * @see #waitForReplicaAddress
     */
    private Address getReplicaAddress(int partitionId, int replicaNumber, int backupCount) {
        IPartition partition = partitionService.getPartition(partitionId);
        Address replicaAddress = partition.getReplicaAddress(replicaNumber);
        if (replicaAddress == null) {
            replicaAddress = waitForReplicaAddress(replicaNumber, partition, backupCount);
        }
        return replicaAddress;
    }

    /**
     * Waits partition table update to get replica address if current replica address is null.
     */
    private Address waitForReplicaAddress(int replica, IPartition partition, int backupCount) {
        int tryCount = RETRY_COUNT;
        Address replicaAddress = null;
        while (replicaAddress == null && partitionService.getMaxAllowedBackupCount() >= backupCount && tryCount-- > 0) {
            sleep();
            replicaAddress = partition.getReplicaAddress(replica);
        }
        return replicaAddress;
    }

    private static void sleep() {
        try {
            MILLISECONDS.sleep(WAIT_PARTITION_TABLE_UPDATE_MILLIS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void addNearCacheStats(String mapName, LocalMapStatsImpl localMapStats,
                                   LocalMapOnDemandCalculatedStats onDemandStats) {

        NearCache nearCache = mapNearCacheManager.getNearCache(mapName);
        if (nearCache == null) {
            return;
        }

        NearCacheStats nearCacheStats = nearCache.getNearCacheStats();

        localMapStats.setNearCacheStats(nearCacheStats);
        onDemandStats.incrementHeapCost(nearCacheStats.getOwnedEntryMemoryCost());
    }

    private void addIndexStats(String mapName, LocalMapStatsImpl localMapStats) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Indexes globalIndexes = mapContainer.getIndexes();

        Map<String, OnDemandIndexStats> freshStats = null;
        if (globalIndexes != null) {
            assert globalIndexes.isGlobal();
            localMapStats.setQueryCount(globalIndexes.getIndexesStats().getQueryCount());
            localMapStats.setIndexedQueryCount(globalIndexes.getIndexesStats().getIndexedQueryCount());
            freshStats = aggregateFreshIndexStats(globalIndexes.getIndexes(), null);
            finalizeFreshIndexStats(freshStats);
        } else {
            long queryCount = 0;
            long indexedQueryCount = 0;
            PartitionContainer[] partitionContainers = mapServiceContext.getPartitionContainers();
            for (PartitionContainer partitionContainer : partitionContainers) {
                IPartition partition = partitionService.getPartition(partitionContainer.getPartitionId());
                if (!partition.isLocal()) {
                    continue;
                }

                Indexes partitionIndexes = partitionContainer.getIndexes().get(mapName);
                if (partitionIndexes == null) {
                    continue;
                }
                assert !partitionIndexes.isGlobal();
                IndexesStats indexesStats = partitionIndexes.getIndexesStats();

                // Partitions may have different query stats due to migrations
                // (partition stats is not preserved while migrating) and/or
                // partition-specific queries, map query stats is estimated as a
                // maximum among partitions.
                queryCount = Math.max(queryCount, indexesStats.getQueryCount());
                indexedQueryCount = Math.max(indexedQueryCount, indexesStats.getIndexedQueryCount());

                freshStats = aggregateFreshIndexStats(partitionIndexes.getIndexes(), freshStats);
            }

            localMapStats.setQueryCount(queryCount);
            localMapStats.setIndexedQueryCount(indexedQueryCount);

            finalizeFreshIndexStats(freshStats);
        }

        localMapStats.updateIndexStats(freshStats);
    }

    private static Map<String, OnDemandIndexStats> aggregateFreshIndexStats(InternalIndex[] freshIndexes,
                                                                            Map<String, OnDemandIndexStats> freshStats) {
        if (freshIndexes.length > 0 && freshStats == null) {
            freshStats = new HashMap<>();
        }

        for (InternalIndex index : freshIndexes) {
            String indexName = index.getName();
            OnDemandIndexStats freshIndexStats = freshStats.get(indexName);
            if (freshIndexStats == null) {
                freshIndexStats = new OnDemandIndexStats();
                freshIndexStats.setCreationTime(Long.MAX_VALUE);
                freshStats.put(indexName, freshIndexStats);
            }

            PerIndexStats indexStats = index.getPerIndexStats();
            freshIndexStats.setCreationTime(Math.min(freshIndexStats.getCreationTime(), indexStats.getCreationTime()));
            long hitCount = indexStats.getHitCount();
            freshIndexStats.setHitCount(Math.max(freshIndexStats.getHitCount(), hitCount));
            freshIndexStats.setQueryCount(Math.max(freshIndexStats.getQueryCount(), indexStats.getQueryCount()));
            freshIndexStats.setMemoryCost(freshIndexStats.getMemoryCost() + indexStats.getMemoryCost());

            freshIndexStats.setAverageHitSelectivity(
                    freshIndexStats.getAverageHitSelectivity() + indexStats.getTotalNormalizedHitCardinality());
            freshIndexStats.setAverageHitLatency(freshIndexStats.getAverageHitLatency() + indexStats.getTotalHitLatency());
            freshIndexStats.setTotalHitCount(freshIndexStats.getTotalHitCount() + hitCount);

            freshIndexStats.setInsertCount(freshIndexStats.getInsertCount() + indexStats.getInsertCount());
            freshIndexStats.setTotalInsertLatency(freshIndexStats.getTotalInsertLatency() + indexStats.getTotalInsertLatency());
            freshIndexStats.setUpdateCount(freshIndexStats.getUpdateCount() + indexStats.getUpdateCount());
            freshIndexStats.setTotalUpdateLatency(freshIndexStats.getTotalUpdateLatency() + indexStats.getTotalUpdateLatency());
            freshIndexStats.setRemoveCount(freshIndexStats.getRemoveCount() + indexStats.getRemoveCount());
            freshIndexStats.setTotalRemoveLatency(freshIndexStats.getTotalRemoveLatency() + indexStats.getTotalRemoveLatency());
        }

        return freshStats;
    }

    /**
     * Finalizes the aggregation of the freshly obtained on-demand index
     * statistics by computing the final average values which are accumulated
     * as total sums in {@link #aggregateFreshIndexStats}.
     *
     * @param freshStats the fresh stats to finalize, can be {@code null} if no
     *                   stats was produced during the aggregation.
     */
    private static void finalizeFreshIndexStats(Map<String, OnDemandIndexStats> freshStats) {
        if (freshStats == null) {
            return;
        }

        for (OnDemandIndexStats freshIndexStats : freshStats.values()) {
            long totalHitCount = freshIndexStats.getTotalHitCount();
            if (totalHitCount != 0) {
                double averageHitSelectivity = 1.0 - freshIndexStats.getAverageHitSelectivity() / totalHitCount;
                averageHitSelectivity = Math.max(0.0, averageHitSelectivity);
                freshIndexStats.setAverageHitSelectivity(averageHitSelectivity);
                freshIndexStats.setAverageHitLatency(freshIndexStats.getAverageHitLatency() / totalHitCount);
            }
        }
    }

    protected static class LocalMapOnDemandCalculatedStats {

        private int backupCount;
        private long hits;
        private long ownedEntryCount;
        private long backupEntryCount;
        private long ownedEntryMemoryCost;
        private long backupEntryMemoryCost;
        // Holds total heap cost of map & Near Cache & backups & merkle trees.
        private long heapCost;
        private long merkleTreesCost;
        private long lockedEntryCount;
        private long dirtyEntryCount;
        private long lastAccessTime;
        private long lastUpdateTime;

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

        public void incrementMerkleTreesCost(long merkleTreeCost) {
            this.merkleTreesCost += merkleTreeCost;
        }

        public LocalMapStatsImpl updateAndGet(LocalMapStatsImpl stats) {
            stats.setBackupCount(backupCount);
            stats.setHits(hits);
            stats.setOwnedEntryCount(ownedEntryCount);
            stats.setBackupEntryCount(backupEntryCount);
            stats.setOwnedEntryMemoryCost(ownedEntryMemoryCost);
            stats.setBackupEntryMemoryCost(backupEntryMemoryCost);
            stats.setHeapCost(heapCost);
            stats.setMerkleTreesCost(merkleTreesCost);
            stats.setLockedEntryCount(lockedEntryCount);
            stats.setDirtyEntryCount(dirtyEntryCount);
            stats.setLastAccessTime(lastAccessTime);
            stats.setLastUpdateTime(lastUpdateTime);
            return stats;
        }

        public void setLastAccessTime(long lastAccessTime) {
            if (lastAccessTime > this.lastAccessTime) {
                this.lastAccessTime = lastAccessTime;
            }
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            if (lastUpdateTime > this.lastUpdateTime) {
                this.lastUpdateTime = lastUpdateTime;
            }

        }
    }
}

package com.hazelcast.map;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordStatistics;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Provides node local statistics of a map.
 */
public class LocalMapStatsProvider {

    private static final int WAIT_PARTITION_TABLE_UPDATE_MILLIS = 100;
    private static final int RETRY_COUNT = 3;

    private final ConcurrentMap<String, LocalMapStatsImpl> statsMap
            = new ConcurrentHashMap<String, LocalMapStatsImpl>(1000);
    private final ConstructorFunction<String, LocalMapStatsImpl> constructorFunction
            = new ConstructorFunction<String, LocalMapStatsImpl>() {
        public LocalMapStatsImpl createNew(String key) {
            return new LocalMapStatsImpl();
        }
    };

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public LocalMapStatsProvider(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = nodeEngine;
    }

    public LocalMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final LocalMapStatsImpl localMapStats = getLocalMapStatsImpl(mapName);
        if (!mapContainer.getMapConfig().isStatisticsEnabled()) {
            return localMapStats;
        }
        final int backupCount = mapContainer.getTotalBackupCount();
        final ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final Address thisAddress = clusterService.getThisAddress();

        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            InternalPartition partition = partitionService.getPartition(partitionId);
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                //no-op because no owner is set yet. Therefor we don't know anything about the map
                continue;
            }
            if (owner.equals(thisAddress)) {
                addOwnerPartitionStats(localMapStats, mapName, partitionId);
            } else {
                addReplicaPartitionStats(localMapStats, mapName, partitionId,
                        partition, clusterService, backupCount, thisAddress);
            }
        }

        localMapStats.setBackupCount(backupCount);

        addNearCacheStats(localMapStats, mapContainer);

        return localMapStats;
    }


    /**
     * Calculates and adds owner partition stats.
     */
    private void addOwnerPartitionStats(LocalMapStatsImpl localMapStats, String mapName, int partitionId) {
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        final RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
        if (recordStore == null) {
            return;
        }
        int lockedEntryCount = 0;
        long lastAccessTime = 0;
        long lastUpdateTime = 0;
        long ownedEntryMemoryCost = 0;
        long hits = 0;
        final Map<Data, Record> records = recordStore.getReadonlyRecordMap();

        localMapStats.incrementHeapCost(recordStore.getHeapCost());
        localMapStats.incrementOwnedEntryCount(records.size());
        for (Record record : records.values()) {
            RecordStatistics stats = record.getStatistics();
            ownedEntryMemoryCost += record.getCost();
            lastAccessTime = Math.max(lastAccessTime, record.getLastAccessTime());
            lastUpdateTime = Math.max(lastUpdateTime, record.getLastUpdateTime());
            hits += stats.getHits();
            if (recordStore.isLocked(record.getKey())) {
                lockedEntryCount++;
            }
        }
        localMapStats.incrementOwnedEntryMemoryCost(ownedEntryMemoryCost);
        localMapStats.incrementLockedEntryCount(lockedEntryCount);
        localMapStats.incrementHits(hits);
        localMapStats.incrementDirtyEntryCount(recordStore.getMapDataStore().notFinishedOperationsCount());
        localMapStats.setLastAccessTime(lastAccessTime);
        localMapStats.setLastUpdateTime(lastUpdateTime);
    }

    /**
     * Calculates and adds replica partition stats.
     */
    private void addReplicaPartitionStats(LocalMapStatsImpl localMapStats, String mapName, int partitionId,
                                          InternalPartition partition, ClusterService clusterService,
                                          int backupCount, Address thisAddress) {
        long heapCost = 0;
        long backupEntryCount = 0;
        long backupEntryMemoryCost = 0;

        for (int replica = 1; replica <= backupCount; replica++) {
            final Address replicaAddress = getReplicaAddress(replica, partition, clusterService, backupCount);
            if (replicaAddress == null && clusterService.getSize() > backupCount) {
                nodeEngine.getLogger(getClass()).warning("Partition: " + partition
                        + ", replica: " + replica + " has no owner!");
                continue;
            }
            if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                heapCost += recordStore.getHeapCost();

                Map<Data, Record> records = recordStore.getReadonlyRecordMap();
                backupEntryCount += records.size();
                for (Record record : records.values()) {
                    backupEntryMemoryCost += record.getCost();
                }
            }
        }
        localMapStats.incrementHeapCost(heapCost);
        localMapStats.incrementBackupEntryCount(backupEntryCount);
        localMapStats.incrementBackupEntryMemoryCost(backupEntryMemoryCost);
    }

    /**
     * Gets replica address. Waits if necessary.
     *
     * @see #waitForReplicaAddress
     */
    private Address getReplicaAddress(int replica, InternalPartition partition,
                                      ClusterService clusterService, int backupCount) {
        Address replicaAddress = partition.getReplicaAddress(replica);
        if (replicaAddress == null) {
            replicaAddress = waitForReplicaAddress(replica, partition, clusterService, backupCount);
        }
        return replicaAddress;
    }

    /**
     * Waits partition table update to get replica address if current replica address is null.
     */
    private Address waitForReplicaAddress(int replica, InternalPartition partition,
                                          ClusterService clusterService, int backupCount) {
        int tryCount = RETRY_COUNT;
        Address replicaAddress = null;
        while (replicaAddress == null && clusterService.getSize() > backupCount && tryCount-- > 0) {
            sleep();
            replicaAddress = partition.getReplicaAddress(replica);
        }
        return replicaAddress;
    }

    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_PARTITION_TABLE_UPDATE_MILLIS);
        } catch (InterruptedException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    /**
     * Adds near cache stats.
     */
    private void addNearCacheStats(LocalMapStatsImpl localMapStats, MapContainer mapContainer) {
        if (!mapContainer.getMapConfig().isNearCacheEnabled()) {
            return;
        }
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        final NearCacheStatsImpl nearCacheStats = nearCacheProvider.getNearCache(mapContainer.getName()).getNearCacheStats();
        final long nearCacheHeapCost = mapContainer.getNearCacheSizeEstimator().getSize();

        localMapStats.setNearCacheStats(nearCacheStats);
        localMapStats.incrementHeapCost(nearCacheHeapCost);
    }

}

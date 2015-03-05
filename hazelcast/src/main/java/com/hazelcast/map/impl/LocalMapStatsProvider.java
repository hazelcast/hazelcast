package com.hazelcast.map.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordStatistics;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.InstantLocalMapStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Provides node local statistics of a map via {@link #createLocalMapStats}
 * and also holds all {@link com.hazelcast.monitor.impl.InstantLocalMapStats} implementations of all maps.
 */
public class LocalMapStatsProvider {

    private static final int WAIT_PARTITION_TABLE_UPDATE_MILLIS = 100;
    private static final int RETRY_COUNT = 3;

    private final ConcurrentMap<String, InstantLocalMapStats> statsMap
            = new ConcurrentHashMap<String, InstantLocalMapStats>(1000);
    private final ConstructorFunction<String, InstantLocalMapStats> constructorFunction
            = new ConstructorFunction<String, InstantLocalMapStats>() {
        public InstantLocalMapStats createNew(String key) {
            return new InstantLocalMapStats();
        }
    };

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public LocalMapStatsProvider(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = nodeEngine;
    }

    public InstantLocalMapStats getInstantLocalMapStats(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public LocalMapStats createLocalMapStats(String mapName) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final LocalMapStatsImpl localMapStatsImpl = new LocalMapStatsImpl(getInstantLocalMapStats(mapName));

        if (!mapContainer.getMapConfig().isStatisticsEnabled()) {
            return localMapStatsImpl;
        }

        final int backupCount = mapContainer.getTotalBackupCount();
        final ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final Address thisAddress = clusterService.getThisAddress();

        localMapStatsImpl.setBackupCount(backupCount);
        addNearCacheStats(localMapStatsImpl, mapContainer);

        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            InternalPartition partition = partitionService.getPartition(partitionId);
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                //no-op because no owner is set yet. Therefor we don't know anything about the map
                continue;
            }
            if (owner.equals(thisAddress)) {
                addOwnerPartitionStats(localMapStatsImpl, mapName, partitionId);
            } else {
                addReplicaPartitionStats(localMapStatsImpl, mapName, partitionId,
                        partition, partitionService, backupCount, thisAddress);
            }
        }

        return localMapStatsImpl;
    }


    /**
     * Calculates and adds owner partition stats.
     */
    private void addOwnerPartitionStats(LocalMapStatsImpl localMapStatsImpl, String mapName, int partitionId) {
        final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
        if (!hasRecords(recordStore)) {
            return;
        }
        int lockedEntryCount = 0;
        long lastAccessTime = 0;
        long lastUpdateTime = 0;
        long ownedEntryMemoryCost = 0;
        long hits = 0;

        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            hits += getHits(record);
            ownedEntryMemoryCost += record.getCost();
            lockedEntryCount += isLocked(record, recordStore);
            lastAccessTime = Math.max(lastAccessTime, record.getLastAccessTime());
            lastUpdateTime = Math.max(lastUpdateTime, record.getLastUpdateTime());
        }

        localMapStatsImpl.incrementOwnedEntryMemoryCost(ownedEntryMemoryCost);
        localMapStatsImpl.incrementLockedEntryCount(lockedEntryCount);
        localMapStatsImpl.incrementHits(hits);
        localMapStatsImpl.incrementDirtyEntryCount(recordStore.getMapDataStore().notFinishedOperationsCount());
        localMapStatsImpl.setLastAccessTime(lastAccessTime);
        localMapStatsImpl.setLastUpdateTime(lastUpdateTime);
        localMapStatsImpl.incrementHeapCost(recordStore.getHeapCost());
        localMapStatsImpl.incrementOwnedEntryCount(recordStore.size());
    }

    private long getHits(Record record) {
        final RecordStatistics stats = record.getStatistics();
        return stats.getHits();
    }

    /**
     * Return 1 if locked, otherwise 0.
     * Used to find {@link com.hazelcast.monitor.impl.InstantLocalMapStats#lockedEntryCount}.
     */
    private int isLocked(Record record, RecordStore recordStore) {
        if (recordStore.isLocked(record.getKey())) {
            return 1;
        }
        return 0;
    }

    /**
     * Calculates and adds replica partition stats.
     */
    private void addReplicaPartitionStats(LocalMapStatsImpl localMapStatsImpl, String mapName, int partitionId,
                                          InternalPartition partition, InternalPartitionService partitionService,
                                          int backupCount, Address thisAddress) {
        long heapCost = 0;
        long backupEntryCount = 0;
        long backupEntryMemoryCost = 0;

        for (int replica = 1; replica <= backupCount; replica++) {
            final Address replicaAddress = getReplicaAddress(replica, partition, partitionService, backupCount);
            if (notGotReplicaAddress(replicaAddress, partitionService, backupCount)) {
                printWarning(partition, replica);
                continue;
            }
            if (gotReplicaAddress(replicaAddress, thisAddress)) {
                RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
                if (hasRecords(recordStore)) {
                    heapCost += recordStore.getHeapCost();
                    backupEntryCount += recordStore.size();
                    backupEntryMemoryCost += getMemoryCost(recordStore);
                }
            }
        }
        localMapStatsImpl.incrementHeapCost(heapCost);
        localMapStatsImpl.incrementBackupEntryCount(backupEntryCount);
        localMapStatsImpl.incrementBackupEntryMemoryCost(backupEntryMemoryCost);
    }

    private boolean hasRecords(RecordStore recordStore) {
        return recordStore != null && recordStore.size() > 0;
    }

    private boolean notGotReplicaAddress(Address replicaAddress, InternalPartitionService partitionService, int backupCount) {
        return replicaAddress == null && partitionService.getMemberGroupsSize() > backupCount;
    }

    private boolean gotReplicaAddress(Address replicaAddress, Address thisAddress) {
        return replicaAddress != null && replicaAddress.equals(thisAddress);
    }

    private void printWarning(InternalPartition partition, int replica) {
        nodeEngine.getLogger(getClass()).warning("Partition: " + partition
                + ", replica: " + replica + " has no owner!");
    }

    private long getMemoryCost(RecordStore recordStore) {
        final Iterator<Record> iterator = recordStore.iterator();
        long cost = 0L;
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            cost += record.getCost();
        }
        return cost;
    }

    private RecordStore getRecordStoreOrNull(String mapName, int partitionId) {
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        return partitionContainer.getExistingRecordStore(mapName);
    }

    /**
     * Gets replica address. Waits if necessary.
     *
     * @see #waitForReplicaAddress
     */
    private Address getReplicaAddress(int replica, InternalPartition partition,
                                      InternalPartitionService partitionService, int backupCount) {
        Address replicaAddress = partition.getReplicaAddress(replica);
        if (replicaAddress == null) {
            replicaAddress = waitForReplicaAddress(replica, partition, partitionService, backupCount);
        }
        return replicaAddress;
    }

    /**
     * Waits partition table update to get replica address if current replica address is null.
     */
    private Address waitForReplicaAddress(int replica, InternalPartition partition,
                                          InternalPartitionService partitionService, int backupCount) {
        int tryCount = RETRY_COUNT;
        Address replicaAddress = null;
        while (replicaAddress == null && partitionService.getMemberGroupsSize() > backupCount && tryCount-- > 0) {
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
    private void addNearCacheStats(LocalMapStatsImpl localMapStatsImpl, MapContainer mapContainer) {
        if (!mapContainer.getMapConfig().isNearCacheEnabled()) {
            return;
        }
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        final NearCacheStats nearCacheStats = nearCacheProvider.getNearCache(mapContainer.getName()).getNearCacheStats();
        final long nearCacheHeapCost = mapContainer.getNearCacheSizeEstimator().getSize();

        localMapStatsImpl.setNearCacheStats(nearCacheStats);
        localMapStatsImpl.incrementHeapCost(nearCacheHeapCost);
    }

}

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

public class LocalMapStatsProvider {

    private static final int WAIT_PARTITION_TABLE_UPDATE_MILLIS = 100;
    private static final int RETRY_COUNT = 3;

    private final ConcurrentMap<String, LocalMapStatsImpl> statsMap = new ConcurrentHashMap<String, LocalMapStatsImpl>(1000);
    private final ConstructorFunction<String, LocalMapStatsImpl> localMapStatsConstructorFunction = new ConstructorFunction<String, LocalMapStatsImpl>() {
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
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localMapStatsConstructorFunction);
    }

    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        final LocalMapStatsImpl localMapStats = mapServiceContext.getLocalMapStatsImpl(mapName);
        if (!mapContainer.getMapConfig().isStatisticsEnabled()) {
            return localMapStats;
        }

        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long dirtyCount = 0;
        long ownedEntryMemoryCost = 0;
        long backupEntryMemoryCost = 0;
        long hits = 0;
        long lockedEntryCount = 0;
        long heapCost = 0;

        int backupCount = mapContainer.getTotalBackupCount();
        ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();

        Address thisAddress = clusterService.getThisAddress();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            InternalPartition partition = partitionService.getPartition(partitionId);
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                //no-op because no owner is set yet. Therefor we don't know anything about the map
                continue;
            }
            if (owner.equals(thisAddress)) {
                PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
                //we don't want to force loading the record store because we are loading statistics. So that is why
                //we ask for 'getExistingRecordStore' instead of 'getRecordStore' which does the load.
                if (recordStore != null) {
                    heapCost += recordStore.getHeapCost();
                    Map<Data, Record> records = recordStore.getReadonlyRecordMap();
                    for (Record record : records.values()) {
                        RecordStatistics stats = record.getStatistics();
                        // there is map store and the record is dirty (waits to be stored)
                        ownedEntryCount++;
                        ownedEntryMemoryCost += record.getCost();
                        localMapStats.setLastAccessTime(record.getLastAccessTime());
                        localMapStats.setLastUpdateTime(record.getLastUpdateTime());
                        hits += stats.getHits();
                        if (recordStore.isLocked(record.getKey())) {
                            lockedEntryCount++;
                        }
                    }
                    dirtyCount += recordStore.getMapDataStore().notFinishedOperationsCount();
                }
            } else {
                for (int replica = 1; replica <= backupCount; replica++) {
                    Address replicaAddress = partition.getReplicaAddress(replica);
                    int tryCount = RETRY_COUNT;
                    // wait if the partition table is not updated yet
                    while (replicaAddress == null && clusterService.getSize() > backupCount && tryCount-- > 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(WAIT_PARTITION_TABLE_UPDATE_MILLIS);
                        } catch (InterruptedException e) {
                            throw ExceptionUtil.rethrow(e);
                        }
                        replicaAddress = partition.getReplicaAddress(replica);
                    }

                    if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                        RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                        heapCost += recordStore.getHeapCost();

                        Map<Data, Record> records = recordStore.getReadonlyRecordMap();
                        for (Record record : records.values()) {
                            backupEntryCount++;
                            backupEntryMemoryCost += record.getCost();
                        }
                    } else if (replicaAddress == null && clusterService.getSize() > backupCount) {
                        nodeEngine.getLogger(getClass()).warning("Partition: " + partition + ", replica: " + replica + " has no owner!");
                    }
                }
            }
        }

        localMapStats.setBackupCount(backupCount);
        localMapStats.setDirtyEntryCount(zeroOrPositive(dirtyCount));
        localMapStats.setLockedEntryCount(zeroOrPositive(lockedEntryCount));
        localMapStats.setHits(zeroOrPositive(hits));
        localMapStats.setOwnedEntryCount(zeroOrPositive(ownedEntryCount));
        localMapStats.setBackupEntryCount(zeroOrPositive(backupEntryCount));
        localMapStats.setOwnedEntryMemoryCost(zeroOrPositive(ownedEntryMemoryCost));
        localMapStats.setBackupEntryMemoryCost(zeroOrPositive(backupEntryMemoryCost));
        // add near cache heap cost.
        heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
        localMapStats.setHeapCost(heapCost);
        if (mapContainer.getMapConfig().isNearCacheEnabled()) {
            final NearCacheStatsImpl nearCacheStats
                    = mapServiceContext.getNearCacheProvider().getNearCache(mapName).getNearCacheStats();
            localMapStats.setNearCacheStats(nearCacheStats);
        }

        return localMapStats;
    }

    private static long zeroOrPositive(long value) {
        return (value > 0) ? value : 0;
    }
}

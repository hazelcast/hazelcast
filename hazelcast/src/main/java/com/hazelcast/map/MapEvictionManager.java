package com.hazelcast.map;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.operation.EvictKeysOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.EmptyArrays;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MapEvictionManager {

    private final MapService mapService;

    public MapEvictionManager(MapService mapService) {
        this.mapService = mapService;
    }

    public void init() {
        mapService.getNodeEngine().getExecutionService()
                .scheduleAtFixedRate(new MapEvictTask(), 1, 1, TimeUnit.SECONDS);
    }

    // todo map evict task is called every second. if load is very high, is it problem? if it is, you can count map-wide puts and fire map-evict in every thousand put
    // todo another "maybe" optimization run clear operation for all maps not just one map
    // todo what if eviction do not complete in 1 second
    private class MapEvictTask implements Runnable {

        private static final String EXECUTOR_NAME = "hz:map-evict";

        public MapEvictTask() {
        }

        @Override
        public void run() {
            final MapService mapService = MapEvictionManager.this.mapService;
            final Map<String, MapContainer> mapContainers = mapService.getMapContainers();
            for (MapContainer mapContainer : mapContainers.values()) {
                final MapConfig.EvictionPolicy evictionPolicy = mapContainer.getMapConfig().getEvictionPolicy();
                final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
                if (MapConfig.EvictionPolicy.NONE.equals(evictionPolicy) || maxSizeConfig.getSize() <= 0) {
                    return;
                }
                final boolean evictable = checkEvictable(mapContainer);
                if (!evictable) {
                    return;
                }
                evictMap(mapContainer);
            }
        }

        private void evictMap(MapContainer mapContainer) {
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final MapConfig mapConfig = mapContainer.getMapConfig();
            for (int i = 0; i < ExecutorConfig.DEFAULT_POOL_SIZE; i++) {
                final EvictRunner runner = new EvictRunner(mapConfig, i);
                nodeEngine.getExecutionService().execute(EXECUTOR_NAME, runner);
            }
        }

        private boolean checkEvictable(MapContainer mapContainer) {
            final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
            final MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
            boolean result;
            switch (maxSizePolicy) {
                case PER_NODE:
                    result = isEvictablePerNode(mapContainer);
                    break;
                case PER_PARTITION:
                    result = isEvictablePerPartition(mapContainer);
                    break;
                case USED_HEAP_PERCENTAGE:
                    result = isEvictableHeapPercentage(mapContainer);
                    break;
                case USED_HEAP_SIZE:
                    result = isEvictableHeapSize(mapContainer);
                    break;
                default:
                    throw new IllegalArgumentException("Not an appropriate max size policy [" + maxSizePolicy + ']');
            }
            return result;
        }

        private boolean isEvictablePerNode(MapContainer mapContainer) {
            int nodeTotalSize = 0;
            final MapService mapService = MapEvictionManager.this.mapService;
            final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
            final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
            final String mapName = mapContainer.getName();
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final InternalPartitionService partitionService = nodeEngine.getPartitionService();
            final int partitionCount = partitionService.getPartitionCount();
            for (int i = 0; i < partitionCount; i++) {
                final Address owner = partitionService.getPartitionOwner(i);
                if (nodeEngine.getThisAddress().equals(owner)) {
                    final PartitionContainer container = mapService.getPartitionContainer(i);
                    if (container == null) {
                        return false;
                    }
                    nodeTotalSize += container.getRecordStore(mapName).size();
                    if (nodeTotalSize >= maxSize) {
                        return true;
                    }
                }
            }
            return false;
        }
        /**
         * used when deciding evictable or not.
         */
        private int getApproximateMaxSize(int maxSizeFromConfig) {
            // because not to exceed the max size much we start eviction early.
            // so decrease the max size with ratio .95 below
            return maxSizeFromConfig * 95 / 100;
        }

        private boolean isEvictablePerPartition(final MapContainer mapContainer) {
            final MapService mapService = MapEvictionManager.this.mapService;
            final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
            final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
            final String mapName = mapContainer.getName();
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final InternalPartitionService partitionService = nodeEngine.getPartitionService();
            for (int i = 0; i < partitionService.getPartitionCount(); i++) {
                final Address owner = partitionService.getPartitionOwner(i);
                if (nodeEngine.getThisAddress().equals(owner)) {
                    final PartitionContainer container = mapService.getPartitionContainer(i);
                    if (container == null) {
                        return false;
                    }
                    final int size = container.getRecordStore(mapName).size();
                    if (size >= maxSize) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isEvictableHeapSize(final MapContainer mapContainer) {
            final long usedHeapSize = getUsedHeapSize(mapContainer);
            if (usedHeapSize == -1L) {
                return false;
            }
            final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
            final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
            return maxSize < (usedHeapSize / 1024 / 1024);
        }

        private boolean isEvictableHeapPercentage(final MapContainer mapContainer) {
            final long usedHeapSize = getUsedHeapSize(mapContainer);
            if (usedHeapSize == -1L) {
                return false;
            }
            final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
            final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
            final long total = Runtime.getRuntime().totalMemory();
            final boolean result = maxSize < (100d * usedHeapSize / total);
            return result;
        }

        private long getUsedHeapSize(final MapContainer mapContainer) {
            long heapCost = 0L;
            final MapService mapService = MapEvictionManager.this.mapService;
            final String mapName = mapContainer.getName();
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final Address thisAddress = nodeEngine.getThisAddress();
            for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
                if (nodeEngine.getPartitionService().getPartition(i).isOwnerOrBackup(thisAddress)) {
                    final PartitionContainer container = mapService.getPartitionContainer(i);
                    if (container == null) {
                        return -1;
                    }
                    heapCost += container.getRecordStore(mapName).getHeapCost();
                }
            }
            heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
            return heapCost;
        }
    }


    private class EvictRunner implements Runnable {
        private final int mod;
        private final MapConfig mapConfig;

        private EvictRunner(MapConfig mapConfig, int mode) {
            this.mod = mode;
            this.mapConfig = mapConfig;
        }

        public void run() {
            final MapConfig mapConfig = this.mapConfig;
            final String mapName = mapConfig.getName();
            final MapService mapService = MapEvictionManager.this.mapService;
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            Set<Data> keysGatheredForNearCacheEviction = Collections.emptySet();
            for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
                if ((i % ExecutorConfig.DEFAULT_POOL_SIZE) != mod) {
                    continue;
                }
                final Address owner = nodeEngine.getPartitionService().getPartitionOwner(i);
                if (nodeEngine.getThisAddress().equals(owner)) {
                    final PartitionContainer pc = mapService.getPartitionContainer(i);
                    final RecordStore recordStore = pc.getRecordStore(mapName);
                    final Collection<Record> values = recordStore.getReadonlyRecordMap().values();
                    if (values.isEmpty()) {
                        continue;
                    }
                    final Object[][] evictableKeyValuePairs = getEvictableRecords(recordStore, mapConfig);
                    if (evictableKeyValuePairs.length == 0) {
                        continue;
                    }
                    final Set<Data> keySet = new HashSet<Data>(evictableKeyValuePairs.length);
                    for (final Object[] kvp : evictableKeyValuePairs) {
                        if (kvp[0] != null) {
                            keySet.add((Data) kvp[0]);
                        }
                    }
                    if (keySet.isEmpty()) {
                        continue;
                    }
                    keysGatheredForNearCacheEviction = new HashSet<Data>(keySet.size());
                    //add keys for near cache eviction.
                    keysGatheredForNearCacheEviction.addAll(keySet);
                    //prepare local "evict keys" operation.
                    EvictKeysOperation evictKeysOperation = new EvictKeysOperation(mapName, keySet);
                    evictKeysOperation.setNodeEngine(nodeEngine);
                    evictKeysOperation.setServiceName(MapService.SERVICE_NAME);
                    evictKeysOperation.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                    evictKeysOperation.setPartitionId(i);
                    OperationAccessor.setCallerAddress(evictKeysOperation, nodeEngine.getThisAddress());
                    nodeEngine.getOperationService().executeOperation(evictKeysOperation);
                    for (final Object[] kvp : evictableKeyValuePairs) {
                        if (kvp[0] != null) {
                            mapService.publishEvent(nodeEngine.getThisAddress(), mapName, EntryEventType.EVICTED,
                                    (Data) kvp[0], mapService.toData(kvp[1]), null);
                        }
                    }
                }
            }
            //send invalidation request to all members.
            if (mapService.isNearCacheAndInvalidationEnabled(mapName)) {
                mapService.invalidateAllNearCaches(mapName, keysGatheredForNearCacheEviction);
            }
        }

    }

    private Object[][] getEvictableRecords(final RecordStore recordStore, final MapConfig mapConfig) {
        final int partitionSize = recordStore.size();
        if (partitionSize < 1) {
            return EmptyArrays.EMPTY_2D_OBJECT_ARRAY;
        }
        final int evictableSize = getEvictableSize(partitionSize, mapConfig);
        if (evictableSize < 1) {
            return EmptyArrays.EMPTY_2D_OBJECT_ARRAY;
        }
        final MapConfig.EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();
        final Map<Data, Record> entries = recordStore.getReadonlyRecordMap();
        final int size = entries.size();
        // size have a tendency to change to here so check again.
        if (entries.isEmpty()) {
            return EmptyArrays.EMPTY_2D_OBJECT_ARRAY;
        }
        // criteria is a long value, like last access times or hits,
        // used for calculating LFU or LRU.
        final long[] criterias = new long[size];
        int index = 0;
        for (final Record record : entries.values()) {
            criterias[index] = getEvictionCriteriaValue(record, evictionPolicy);
            index++;
            //in case size may change (increase or decrease) when iterating.
            if (index == size) {
                break;
            }
        }
        if (criterias.length == 0) {
            return EmptyArrays.EMPTY_2D_OBJECT_ARRAY;
        }
        // just in case there may be unassigned indexes in criterias array due to size variances
        // assign them to Long.MAX_VALUE so when sorting asc they will locate
        // in the upper array indexes and we wont care about them.
        if (index < criterias.length) {
            for (int i = index; i < criterias.length; i++) {
                criterias[i] = Long.MAX_VALUE;
            }
        }
        Arrays.sort(criterias);
        // we do want to hold references to key&value pairs and don't want to depend on Record
        // since Record content may be changed.
        final Object[][] evictableKeyValuePairs = new Object[evictableSize][2];
        int indexKVP = 0;
        // check in case record store size may be smaller than evictable size.
        final int evictableBaseIndex = Math.min(evictableSize, index - 1);
        final long criteriaValue = criterias[evictableBaseIndex];
        for (final Map.Entry<Data, Record> entry : entries.entrySet()) {
            final Record record = entry.getValue();
            final long value = getEvictionCriteriaValue(record, evictionPolicy);
            if (value <= criteriaValue) {
                evictableKeyValuePairs[indexKVP][0] = record.getKey();
                evictableKeyValuePairs[indexKVP][1] = record.getValue();
                indexKVP++;
            }
            if (indexKVP >= evictableSize) {
                break;
            }
        }
        if (evictableKeyValuePairs.length == 0) {
            return EmptyArrays.EMPTY_2D_OBJECT_ARRAY;
        }
        return evictableKeyValuePairs;
    }

    private int getEvictableSize(int currentPartitionSize, MapConfig mapConfig) {
        int evictableSize;
        final MaxSizeConfig.MaxSizePolicy maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
        final int evictionPercentage = mapConfig.getEvictionPercentage();
        switch (maxSizePolicy) {
            case PER_PARTITION:
                int maxSize = mapConfig.getMaxSizeConfig().getSize();
                int targetSizePerPartition = Double.valueOf(maxSize * ((100 - evictionPercentage) / 100.0)).intValue();
                int diffFromTargetSize = currentPartitionSize - targetSizePerPartition;
                int prunedSize = currentPartitionSize * evictionPercentage / 100 + 1;
                evictableSize = Math.max(diffFromTargetSize, prunedSize);
                break;
            case PER_NODE:
                maxSize = mapConfig.getMaxSizeConfig().getSize();
                int memberCount = mapService.getNodeEngine().getClusterService().getMembers().size();
                int maxPartitionSize = (maxSize * memberCount / mapService.getNodeEngine().getPartitionService().getPartitionCount());
                targetSizePerPartition = Double.valueOf(maxPartitionSize * ((100 - evictionPercentage) / 100.0)).intValue();
                diffFromTargetSize = currentPartitionSize - targetSizePerPartition;
                prunedSize = currentPartitionSize * evictionPercentage / 100 + 1;
                evictableSize = Math.max(diffFromTargetSize, prunedSize);
                break;
            case USED_HEAP_PERCENTAGE:
            case USED_HEAP_SIZE:
                evictableSize = currentPartitionSize * evictionPercentage / 100;
                break;
            default:
                throw new IllegalArgumentException("Max size policy is not defined [" + maxSizePolicy + "]");
        }
        return evictableSize;
    }

    private long getEvictionCriteriaValue(Record record, MapConfig.EvictionPolicy evictionPolicy) {
        long value;
        switch (evictionPolicy) {
            case LRU:
            case LFU:
                value = record.getEvictionCriteriaNumber();
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate eviction policy [" + evictionPolicy + ']');
        }
        return value;
    }


}

/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.eviction;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapEventPublisher;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.NearCacheProvider;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Eviction helper methods.
 */
public final class EvictionHelper {

    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int EVICTION_START_THRESHOLD_PERCENTAGE = 95;
    private static final int ONE_KILOBYTE = 1024;

    private EvictionHelper() {
    }

    public static boolean checkEvictable(MapContainer mapContainer) {
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

    public static void removeEvictableRecords(final RecordStore recordStore, int evictableSize, final MapConfig mapConfig,
                                              final MapServiceContext mapServiceContext) {
        final MapConfig.EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();
        // criteria is a long value, like last access times or hits,
        // used for calculating LFU or LRU.
        long[] criterias = createAndPopulateEvictionCriteriaArray(recordStore, evictionPolicy);
        if (criterias == null) {
            return;
        }
        Arrays.sort(criterias);
        // check in case record store size may be smaller than evictable size.
        final int evictableBaseIndex = getEvictionStartIndex(criterias, evictableSize);
        final long criteriaValue = criterias[evictableBaseIndex];
        int evictedRecordCounter = 0;
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            final long value = getEvictionCriteriaValue(record, evictionPolicy);
            if (value <= criteriaValue) {
                final Data tmpKey = record.getKey();
                final Object tmpValue = record.getValue();
                if (evictIfNotLocked(tmpKey, recordStore)) {
                    evictedRecordCounter++;
                    final String mapName = mapConfig.getName();
                    interceptAndInvalidate(mapServiceContext, value, tmpKey, mapName);
                    fireEvent(tmpKey, tmpValue, mapName, mapServiceContext);
                }
            }
            if (evictedRecordCounter >= evictableSize) {
                break;
            }
        }
    }

    private static long[] createAndPopulateEvictionCriteriaArray(RecordStore recordStore,
                                                                 MapConfig.EvictionPolicy evictionPolicy) {
        final int size = recordStore.size();
        long[] criterias = null;
        int index = 0;
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            if (criterias == null) {
                criterias = new long[size];
            }
            criterias[index] = getEvictionCriteriaValue(record, evictionPolicy);
            index++;
            //in case size may change (increase or decrease) when iterating.
            if (index == size) {
                break;
            }
        }
        if (criterias == null) {
            return null;
        }
        // just in case there may be unassigned indexes in criterias array due to size variances
        // assign them to Long.MAX_VALUE so when sorting asc they will locate
        // in the upper array indexes and we wont care about them.
        if (index < criterias.length) {
            for (int i = index; i < criterias.length; i++) {
                criterias[i] = Long.MAX_VALUE;
            }
        }
        return criterias;
    }

    private static int getEvictionStartIndex(long[] criterias, int evictableSize) {
        final int length = criterias.length;
        final int sizeToEvict = Math.min(evictableSize, length);
        final int index = sizeToEvict - 1;
        return index < 0 ? 0 : index;
    }

    private static void interceptAndInvalidate(MapServiceContext mapServiceContext, long value, Data tmpKey, String mapName) {
        mapServiceContext.interceptAfterRemove(mapName, value);
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(mapName)) {
            nearCacheProvider.invalidateAllNearCaches(mapName, tmpKey);
        }
    }

    public static void fireEvent(Data key, Object value, String mapName, MapServiceContext mapServiceContext) {
        final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Address thisAddress = nodeEngine.getThisAddress();
        final Data dataValue = mapServiceContext.toData(value);
        mapEventPublisher.publishEvent(thisAddress, mapName, EntryEventType.EVICTED,
                key, dataValue, null);
    }

    public static boolean evictIfNotLocked(Data key, RecordStore recordStore) {
        if (recordStore.isLocked(key)) {
            return false;
        }
        recordStore.evict(key);
        return true;
    }


    public static int getEvictableSize(int currentPartitionSize, MapConfig mapConfig, MapServiceContext mapServiceContext) {
        int evictableSize;
        final MaxSizeConfig.MaxSizePolicy maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
        final int evictionPercentage = mapConfig.getEvictionPercentage();
        switch (maxSizePolicy) {
            case PER_PARTITION:
                int maxSize = mapConfig.getMaxSizeConfig().getSize();
                int targetSizePerPartition = Double.valueOf(maxSize
                        * ((ONE_HUNDRED_PERCENT - evictionPercentage) / (1D * ONE_HUNDRED_PERCENT))).intValue();
                int diffFromTargetSize = currentPartitionSize - targetSizePerPartition;
                int prunedSize = currentPartitionSize * evictionPercentage / ONE_HUNDRED_PERCENT + 1;
                evictableSize = Math.max(diffFromTargetSize, prunedSize);
                break;
            case PER_NODE:
                maxSize = mapConfig.getMaxSizeConfig().getSize();
                int memberCount = mapServiceContext.getNodeEngine().getClusterService().getMembers().size();
                int maxPartitionSize = (maxSize
                        * memberCount / mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount());
                targetSizePerPartition = Double.valueOf(maxPartitionSize
                        * ((ONE_HUNDRED_PERCENT - evictionPercentage) / (1D * ONE_HUNDRED_PERCENT))).intValue();
                diffFromTargetSize = currentPartitionSize - targetSizePerPartition;
                prunedSize = currentPartitionSize * evictionPercentage / ONE_HUNDRED_PERCENT + 1;
                evictableSize = Math.max(diffFromTargetSize, prunedSize);
                break;
            case USED_HEAP_PERCENTAGE:
            case USED_HEAP_SIZE:
                evictableSize = currentPartitionSize * evictionPercentage / ONE_HUNDRED_PERCENT;
                break;
            default:
                throw new IllegalArgumentException("Max size policy is not defined [" + maxSizePolicy + "]");
        }
        return evictableSize;
    }

    private static long getEvictionCriteriaValue(Record record, MapConfig.EvictionPolicy evictionPolicy) {
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


    private static boolean isEvictablePerNode(MapContainer mapContainer) {
        int nodeTotalSize = 0;
        final MapService mapService = mapContainer.getMapServiceContext().getService();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final String mapName = mapContainer.getName();
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionCount = partitionService.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            final Address owner = partitionService.getPartitionOwner(i);
            if (nodeEngine.getThisAddress().equals(owner)) {
                final PartitionContainer container = mapService.getMapServiceContext().getPartitionContainer(i);
                if (container == null) {
                    return false;
                }
                nodeTotalSize += getRecordStoreSize(mapName, container);
                if (nodeTotalSize >= maxSize) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int getRecordStoreSize(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0;
        }
        return existingRecordStore.size();
    }

    private static long getRecordStoreHeapCost(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0L;
        }
        return existingRecordStore.getHeapCost();
    }

    /**
     * used when deciding evictable or not.
     */
    private static int getApproximateMaxSize(int maxSizeFromConfig) {
        // because not to exceed the max size much we start eviction early.
        // so decrease the max size with ratio .95 below
        return maxSizeFromConfig * EVICTION_START_THRESHOLD_PERCENTAGE / ONE_HUNDRED_PERCENT;
    }

    private static boolean isEvictablePerPartition(final MapContainer mapContainer) {
        final MapService mapService = mapContainer.getMapServiceContext().getService();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final String mapName = mapContainer.getName();
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            final Address owner = partitionService.getPartitionOwner(i);
            if (nodeEngine.getThisAddress().equals(owner)) {
                final PartitionContainer container = mapService.getMapServiceContext().getPartitionContainer(i);
                if (container == null) {
                    return false;
                }
                final int size = getRecordStoreSize(mapName, container);
                if (size >= maxSize) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isEvictableHeapSize(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return maxSize < (usedHeapSize / ONE_KILOBYTE / ONE_KILOBYTE);
    }

    private static boolean isEvictableHeapPercentage(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = Runtime.getRuntime().totalMemory();
        return maxSize < (1D * ONE_HUNDRED_PERCENT * usedHeapSize / total);
    }

    private static long getUsedHeapSize(final MapContainer mapContainer) {
        long heapCost = 0L;
        final MapService mapService = mapContainer.getMapServiceContext().getService();
        final String mapName = mapContainer.getName();
        final NodeEngine nodeEngine = mapService.getMapServiceContext().getNodeEngine();
        final Address thisAddress = nodeEngine.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            InternalPartition partition = nodeEngine.getPartitionService().getPartition(i, false);
            if (partition.isOwnerOrBackup(thisAddress)) {
                final PartitionContainer container = mapService.getMapServiceContext().getPartitionContainer(i);
                if (container == null) {
                    return -1L;
                }
                heapCost += getRecordStoreHeapCost(mapName, container);
            }
        }
        heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
        return heapCost;
    }

}

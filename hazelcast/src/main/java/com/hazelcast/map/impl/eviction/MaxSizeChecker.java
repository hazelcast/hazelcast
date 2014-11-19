/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy}
 * to start eviction process.
 *
 * @see com.hazelcast.map.impl.eviction.EvictionOperator#maxSizeChecker
 */
public class MaxSizeChecker {

    private static final int ONE_HUNDRED_PERCENT = 100;
    private static final int EVICTION_START_THRESHOLD_PERCENTAGE = 95;
    private static final int ONE_KILOBYTE = 1024;
    private static final int ONE_MEGABYTE = ONE_KILOBYTE * ONE_KILOBYTE;

    private final MemoryInfoAccessor memoryInfoAccessor;
    private final MapServiceContext mapServiceContext;


    public MaxSizeChecker(MapServiceContext mapServiceContext) {
        this(new RuntimeMemoryInfoAccessor(), mapServiceContext);
    }

    public MaxSizeChecker(MemoryInfoAccessor memoryInfoAccessor, MapServiceContext mapServiceContext) {
        this.memoryInfoAccessor = memoryInfoAccessor;
        this.mapServiceContext = mapServiceContext;
    }

    public boolean checkEvictable(MapContainer mapContainer, int partitionId) {
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        boolean result;
        switch (maxSizePolicy) {
            case PER_NODE:
                result = isEvictablePerNode(mapContainer);
                break;
            case PER_PARTITION:
                result = isEvictablePerPartition(mapContainer, partitionId);
                break;
            case USED_HEAP_PERCENTAGE:
                result = isEvictableHeapPercentage(mapContainer);
                break;
            case USED_HEAP_SIZE:
                result = isEvictableHeapSize(mapContainer);
                break;
            case FREE_HEAP_PERCENTAGE:
                result = isEvictableFreeHeapPercentage(mapContainer);
                break;
            case FREE_HEAP_SIZE:
                result = isEvictableFreeHeapSize(mapContainer);
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate max size policy [" + maxSizePolicy + ']');
        }
        return result;
    }

    private boolean isEvictablePerNode(MapContainer mapContainer) {
        int nodeTotalSize = 0;
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final String mapName = mapContainer.getName();
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final List<Integer> partitionIds = findPartitionIds();
        for (int partitionId : partitionIds) {
            final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
            if (container == null) {
                continue;
            }
            nodeTotalSize += getRecordStoreSize(mapName, container);
            if (nodeTotalSize >= maxSize) {
                return true;
            }
        }
        return false;
    }

    private boolean isEvictablePerPartition(final MapContainer mapContainer, int partitionId) {
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final String mapName = mapContainer.getName();
        final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        if (container == null) {
            return false;
        }
        final int size = getRecordStoreSize(mapName, container);
        return size >= maxSize;
    }

    private boolean isEvictableHeapSize(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return maxSize < (usedHeapSize / ONE_MEGABYTE);
    }

    private boolean isEvictableFreeHeapSize(final MapContainer mapContainer) {
        final long currentFreeHeapSize = getAvailableMemory();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int minFreeHeapSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return minFreeHeapSize > (currentFreeHeapSize / ONE_MEGABYTE);
    }

    private boolean isEvictableHeapPercentage(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = getTotalMemory();
        return maxSize < (1D * ONE_HUNDRED_PERCENT * usedHeapSize / total);
    }

    private boolean isEvictableFreeHeapPercentage(final MapContainer mapContainer) {
        final long currentFreeHeapSize = getAvailableMemory();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final int freeHeapPercentage = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = getTotalMemory();
        return freeHeapPercentage > (1D * ONE_HUNDRED_PERCENT * currentFreeHeapSize / total);
    }

    private long getTotalMemory() {
        return memoryInfoAccessor.getTotalMemory();
    }

    private long getFreeMemory() {
        return memoryInfoAccessor.getFreeMemory();
    }

    private long getMaxMemory() {
        return memoryInfoAccessor.getMaxMemory();
    }

    private long getAvailableMemory() {
        final long totalMemory = getTotalMemory();
        final long freeMemory = getFreeMemory();
        final long maxMemory = getMaxMemory();
        return freeMemory + (maxMemory - totalMemory);
    }

    private long getUsedHeapSize(final MapContainer mapContainer) {
        long heapCost = 0L;
        final String mapName = mapContainer.getName();
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final List<Integer> partitionIds = findPartitionIds();
        for (int partitionId : partitionIds) {
            final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
            if (container == null) {
                continue;
            }
            heapCost += getRecordStoreHeapCost(mapName, container);
        }
        heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
        return heapCost;
    }

    private int getRecordStoreSize(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0;
        }
        return existingRecordStore.size();
    }

    private long getRecordStoreHeapCost(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0L;
        }
        return existingRecordStore.getHeapCost();
    }

    /**
     * used when deciding evictable or not.
     */
    private int getApproximateMaxSize(int maxSizeFromConfig) {
        // because not to exceed the max size much we start eviction early.
        // so decrease the max size with ratio .95 below
        return maxSizeFromConfig * EVICTION_START_THRESHOLD_PERCENTAGE / ONE_HUNDRED_PERCENT;
    }


    private List<Integer> findPartitionIds() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionCount = partitionService.getPartitionCount();
        List<Integer> partitionIds = null;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (isOwnerOrBackup(partitionId)) {
                if (partitionIds == null) {
                    partitionIds = new ArrayList<Integer>();
                }
                partitionIds.add(partitionId);
            }
        }
        return partitionIds == null ? Collections.<Integer>emptyList() : partitionIds;
    }


    private boolean isOwnerOrBackup(int partitionId) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final InternalPartition partition = partitionService.getPartition(partitionId, false);
        final Address thisAddress = nodeEngine.getThisAddress();
        return partition.isOwnerOrBackup(thisAddress);
    }

}

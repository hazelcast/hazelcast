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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.memory.MemoryUnit.BYTES;
import static java.lang.String.format;
import static java.lang.System.getProperty;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy}
 * to start eviction process.
 *
 * @see com.hazelcast.map.impl.eviction.EvictionOperator#maxSizeChecker
 */
public class MaxSizeChecker {

    public static final String HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL = "hazelcast.memory.info.accessor.impl";

    private static final double ONE_HUNDRED_PERCENT = 100D;
    private static final int EVICTION_START_THRESHOLD_PERCENTAGE = 95;
    private static final int ONE_KILOBYTE = 1024;
    private static final int ONE_MEGABYTE = ONE_KILOBYTE * ONE_KILOBYTE;

    private final MemoryInfoAccessor memoryInfoAccessor;
    private final MapServiceContext mapServiceContext;
    private final ILogger logger;

    public MaxSizeChecker(MapServiceContext mapServiceContext) {
        this(new RuntimeMemoryInfoAccessor(), mapServiceContext);
    }

    public MaxSizeChecker(MemoryInfoAccessor memoryInfoAccessor, MapServiceContext mapServiceContext) {
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
        this.memoryInfoAccessor = initMemoryInfoAccessor(memoryInfoAccessor);
        this.mapServiceContext = mapServiceContext;
    }

    private MemoryInfoAccessor initMemoryInfoAccessor(MemoryInfoAccessor given) {
        MemoryInfoAccessor memoryInfoAccessor = given;
        String memoryInfoAccessorImpl = getProperty(HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL);
        if (memoryInfoAccessorImpl != null) {
            try {
                memoryInfoAccessor = ClassLoaderUtil.newInstance(null, memoryInfoAccessorImpl);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Used memoryInfoAccessor=" + memoryInfoAccessor.getClass().getCanonicalName());
        }

        return memoryInfoAccessor;
    }

    public boolean checkEvictable(MapContainer mapContainer, int partitionId) {
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        boolean result;
        switch (maxSizePolicy) {
            case PER_NODE:
                result = isEvictablePerNode(mapContainer, partitionId);
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


    private boolean isEvictablePerNode(MapContainer mapContainer, int partitionId) {
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = container.getExistingRecordStore(mapContainer.getName());
        if (recordStore == null) {
            return false;
        }
        double maxExpectedRecordStoreSize = calculatePerNodeMaxRecordStoreSize(recordStore);
        return recordStore.size() > maxExpectedRecordStoreSize;
    }

    /**
     * Calculates and returns the expected maximum size of an evicted record-store
     * when {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy#PER_NODE PER_NODE} max-size-policy is used.
     */
    public double calculatePerNodeMaxRecordStoreSize(RecordStore recordStore) {
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        int configuredMaxSize = maxSizeConfig.getSize();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int memberCount = nodeEngine.getClusterService().getSize();

        return (1D * configuredMaxSize * memberCount / partitionCount);

    }

    private boolean isEvictablePerPartition(final MapContainer mapContainer, int partitionId) {
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
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
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return maxSize < (1D * usedHeapSize / ONE_MEGABYTE);
    }

    private boolean isEvictableFreeHeapSize(final MapContainer mapContainer) {
        final long currentFreeHeapSize = getAvailableMemory();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double minFreeHeapSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return minFreeHeapSize > (1D * currentFreeHeapSize / ONE_MEGABYTE);
    }

    private boolean isEvictableHeapPercentage(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double maxSize = maxSizeConfig.getSize();
        final long maxMemory = getMaxMemory();
        return maxSize < (ONE_HUNDRED_PERCENT * usedHeapSize / maxMemory);
    }

    private boolean isEvictableFreeHeapPercentage(final MapContainer mapContainer) {
        MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();

        long totalMemory = getTotalMemory();
        long freeMemory = getFreeMemory();
        long maxMemory = getMaxMemory();
        long availableMemory = freeMemory + (maxMemory - totalMemory);

        double configuredPercentage = maxSizeConfig.getSize();
        double actualPercentage = ONE_HUNDRED_PERCENT * availableMemory / maxMemory;

        boolean evictable = configuredPercentage > actualPercentage;
        if (evictable && logger.isFinestEnabled()) {
            String msg = "runtime.max=%s, runtime.used=%s, configuredFree%%=%.2f, actualFree%%=%.2f";
            logger.finest(format(msg, toMB(maxMemory), toMB(totalMemory - freeMemory), configuredPercentage, actualPercentage));
        }

        return evictable;
    }

    private static String toMB(long bytes) {
        return BYTES.toMegaBytes(bytes) + "M";
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
    private static double getApproximateMaxSize(int maxSizeFromConfig) {
        // because not to exceed the max size much we start eviction early.
        // so decrease the max size with ratio .95 below
        return 1D * maxSizeFromConfig * EVICTION_START_THRESHOLD_PERCENTAGE / ONE_HUNDRED_PERCENT;
    }

    /**
     * Get max size setting form config for given policy
     *
     * @return max size or -1 if policy is different or not set
     */
    public static double getApproximateMaxSize(MaxSizeConfig maxSizeConfig, MaxSizePolicy policy) {
        if (maxSizeConfig.getMaxSizePolicy() == policy) {
            return getApproximateMaxSize(maxSizeConfig.getSize());
        }
        return -1D;
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



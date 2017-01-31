/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.MemoryInfoAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.memory.MemorySize.toPrettyString;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy}
 * to start eviction process.
 *
 * @see EvictorImpl#evictionChecker
 */
public class EvictionChecker {

    protected static final double ONE_HUNDRED_PERCENT = 100D;

    protected final ILogger logger;
    protected final MapServiceContext mapServiceContext;
    protected final MemoryInfoAccessor memoryInfoAccessor;

    public EvictionChecker(MemoryInfoAccessor givenMemoryInfoAccessor, MapServiceContext mapServiceContext) {
        checkNotNull(givenMemoryInfoAccessor, "givenMemoryInfoAccessor cannot be null");
        checkNotNull(mapServiceContext, "mapServiceContext cannot be null");

        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
        this.mapServiceContext = mapServiceContext;
        this.memoryInfoAccessor = givenMemoryInfoAccessor;

        if (logger.isFinestEnabled()) {
            logger.finest("Used memoryInfoAccessor=" + this.memoryInfoAccessor.getClass().getCanonicalName());
        }
    }


    public boolean checkEvictable(RecordStore recordStore) {
        if (recordStore.size() == 0) {
            return false;
        }

        String mapName = recordStore.getName();

        MapContainer mapContainer = recordStore.getMapContainer();
        MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();

        switch (maxSizePolicy) {
            case PER_NODE:
                return checkPerNodeEviction(recordStore);
            case PER_PARTITION:
                int partitionId = recordStore.getPartitionId();
                return checkPerPartitionEviction(mapName, maxSizeConfig, partitionId);
            case USED_HEAP_PERCENTAGE:
                return checkHeapPercentageEviction(mapName, maxSizeConfig);
            case USED_HEAP_SIZE:
                return checkHeapSizeEviction(mapName, maxSizeConfig);
            case FREE_HEAP_PERCENTAGE:
                return checkFreeHeapPercentageEviction(maxSizeConfig);
            case FREE_HEAP_SIZE:
                return checkFreeHeapSizeEviction(maxSizeConfig);
            default:
                throw new IllegalArgumentException("Not an appropriate max size policy [" + maxSizePolicy + ']');
        }
    }

    protected boolean checkPerNodeEviction(RecordStore recordStore) {
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
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        int configuredMaxSize = maxSizeConfig.getSize();
        int memberCount = nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR);
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();

        return (1D * configuredMaxSize * memberCount / partitionCount);

    }

    protected boolean checkPerPartitionEviction(String mapName, MaxSizeConfig maxSizeConfig, int partitionId) {
        final double maxSize = maxSizeConfig.getSize();
        final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        if (container == null) {
            return false;
        }
        final int size = getRecordStoreSize(mapName, container);
        return size > maxSize;
    }

    protected boolean checkHeapSizeEviction(String mapName, MaxSizeConfig maxSizeConfig) {
        long usedHeapBytes = getUsedHeapInBytes(mapName);
        if (usedHeapBytes == -1L) {
            return false;
        }

        int maxUsableHeapMegaBytes = maxSizeConfig.getSize();

        return MEGABYTES.toBytes(maxUsableHeapMegaBytes) < usedHeapBytes;
    }

    protected boolean checkFreeHeapSizeEviction(MaxSizeConfig maxSizeConfig) {
        long currentFreeHeapBytes = getAvailableMemory();
        int minFreeHeapMegaBytes = maxSizeConfig.getSize();

        return MEGABYTES.toBytes(minFreeHeapMegaBytes) > currentFreeHeapBytes;
    }

    protected boolean checkHeapPercentageEviction(String mapName, MaxSizeConfig maxSizeConfig) {
        long usedHeapBytes = getUsedHeapInBytes(mapName);
        if (usedHeapBytes == -1L) {
            return false;
        }

        double maxOccupiedHeapPercentage = maxSizeConfig.getSize();
        long maxMemory = getMaxMemory();

        if (maxMemory <= 0) {
            return true;
        }

        return maxOccupiedHeapPercentage < (ONE_HUNDRED_PERCENT * usedHeapBytes / maxMemory);
    }

    protected boolean checkFreeHeapPercentageEviction(MaxSizeConfig maxSizeConfig) {
        long totalMemory = getTotalMemory();
        long freeMemory = getFreeMemory();
        long maxMemory = getMaxMemory();
        long availableMemory = freeMemory + (maxMemory - totalMemory);

        boolean evictable;
        double actualFreePercentage = 0;
        double configuredFreePercentage = 0;

        if (totalMemory <= 0 || freeMemory <= 0 || maxMemory <= 0 || availableMemory <= 0) {
            evictable = true;
        } else {
            actualFreePercentage = ONE_HUNDRED_PERCENT * availableMemory / maxMemory;
            configuredFreePercentage = maxSizeConfig.getSize();

            evictable = configuredFreePercentage > actualFreePercentage;
        }

        if (evictable && logger.isFinestEnabled()) {
            logger.finest(format("runtime.max=%s, runtime.used=%s, configuredFree%%=%.2f, actualFree%%=%.2f",
                    toPrettyString(maxMemory),
                    toPrettyString(totalMemory - freeMemory),
                    configuredFreePercentage,
                    actualFreePercentage));
        }

        return evictable;
    }

    protected long getTotalMemory() {
        return memoryInfoAccessor.getTotalMemory();
    }

    protected long getFreeMemory() {
        return memoryInfoAccessor.getFreeMemory();
    }

    protected long getMaxMemory() {
        return memoryInfoAccessor.getMaxMemory();
    }

    protected long getAvailableMemory() {
        final long totalMemory = getTotalMemory();
        final long freeMemory = getFreeMemory();
        final long maxMemory = getMaxMemory();
        return freeMemory + (maxMemory - totalMemory);
    }

    protected long getUsedHeapInBytes(String mapName) {
        long heapCost = 0L;
        final List<Integer> partitionIds = findPartitionIds();
        for (int partitionId : partitionIds) {
            final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
            if (container == null) {
                continue;
            }
            heapCost += getRecordStoreHeapCost(mapName, container);
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (!mapContainer.getMapConfig().isNearCacheEnabled()) {
            return heapCost;
        }

        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        NearCache nearCache = mapNearCacheManager.getNearCache(mapName);
        NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
        heapCost += nearCacheStats.getOwnedEntryMemoryCost();

        return heapCost;
    }

    protected int getRecordStoreSize(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0;
        }
        return existingRecordStore.size();
    }

    protected long getRecordStoreHeapCost(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0L;
        }
        return existingRecordStore.getOwnedEntryCost();
    }

    protected List<Integer> findPartitionIds() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final IPartitionService partitionService = nodeEngine.getPartitionService();
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

    protected boolean isOwnerOrBackup(int partitionId) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        final IPartition partition = partitionService.getPartition(partitionId, false);
        final Address thisAddress = nodeEngine.getThisAddress();
        return partition.isOwnerOrBackup(thisAddress);
    }
}



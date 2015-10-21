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

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Eviction helper methods.
 */
public class EvictorImpl implements Evictor {

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int EVICTION_START_THRESHOLD_PERCENTAGE = 95;
    protected static final int ONE_KILOBYTE = 1024;
    protected static final int ONE_MEGABYTE = ONE_KILOBYTE * ONE_KILOBYTE;

    protected final MapServiceContext mapServiceContext;

    // not final for testing purposes.
    protected MemoryInfoAccessor memoryInfoAccessor;

    public EvictorImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.memoryInfoAccessor = new RuntimeMemoryInfoAccessor();
    }

    @Override
    public void evict(RecordStore recordStore) {
        if (!recordStore.isEvictionEnabled()
                || !isReachedMaxSize(recordStore)) {
            return;
        }

        int removalSize = calculateRemovalSize(recordStore);
        if (removalSize < 1) {
            return;
        }

        removeRecords(recordStore, removalSize);
    }

    protected void removeRecords(RecordStore recordStore, int sizeToEvict) {
        long now = Clock.currentTimeMillis();
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();

        boolean backup = isBackup(recordStore);

        EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();
        // criteria is a long value, like last access times or hits,
        // used for calculating LFU or LRU.
        long[] criterias = createAndPopulateEvictionCriteriaArray(recordStore, evictionPolicy);
        if (criterias == null) {
            return;
        }
        Arrays.sort(criterias);
        // check in case record store size may be smaller than evictable size.
        int evictableBaseIndex = getEvictionStartIndex(criterias, sizeToEvict);
        long criteriaValue = criterias[evictableBaseIndex];
        int evictedRecordCounter = 0;
        Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            long value = getEvictionCriteriaValue(record, evictionPolicy);
            if (value <= criteriaValue) {
                if (tryEvict(key, record, recordStore, backup, now)) {
                    evictedRecordCounter++;
                }
            }
            if (evictedRecordCounter >= sizeToEvict) {
                break;
            }
        }
    }

    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        InternalPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    protected boolean tryEvict(Data key, Record record, RecordStore recordStore, boolean backup, long now) {
        Object value = record.getValue();

        if (recordStore.isLocked(key)) {
            return false;
        }

        recordStore.evict(key, backup);

        if (!backup) {
            boolean expired = recordStore.isExpired(record, now, false);
            recordStore.doPostEvictionOperations(key, value, expired);
        }

        return true;
    }

    protected long[] createAndPopulateEvictionCriteriaArray(RecordStore recordStore,
                                                            EvictionPolicy evictionPolicy) {
        final int size = recordStore.size();
        long[] criterias = null;
        int index = 0;
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
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

    protected int getEvictionStartIndex(long[] criterias, int evictableSize) {
        final int length = criterias.length;
        final int sizeToEvict = Math.min(evictableSize, length);
        final int index = sizeToEvict - 1;
        return index < 0 ? 0 : index;
    }

    protected int calculateRemovalSize(RecordStore recordStore) {
        int size = recordStore.size();
        if (size == 0) {
            return 0;
        }

        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
        int maxSize = mapConfig.getMaxSizeConfig().getSize();

        int removalSize;
        final MaxSizeConfig.MaxSizePolicy maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
        final int evictionPercentage = mapConfig.getEvictionPercentage();
        switch (maxSizePolicy) {
            case PER_PARTITION:
                int targetSizePerPartition = Double.valueOf(maxSize
                        * ((ONE_HUNDRED_PERCENT - evictionPercentage) / (1D * ONE_HUNDRED_PERCENT))).intValue();
                int diffFromTargetSize = size - targetSizePerPartition;
                int prunedSize = size * evictionPercentage / ONE_HUNDRED_PERCENT + 1;
                removalSize = Math.max(diffFromTargetSize, prunedSize);
                break;
            case PER_NODE:
                int memberCount = mapServiceContext.getNodeEngine().getClusterService()
                        .getSize(DATA_MEMBER_SELECTOR);
                int maxPartitionSize = (maxSize
                        * memberCount / mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount());
                targetSizePerPartition = Double.valueOf(maxPartitionSize
                        * ((ONE_HUNDRED_PERCENT - evictionPercentage) / (1D * ONE_HUNDRED_PERCENT))).intValue();
                diffFromTargetSize = size - targetSizePerPartition;
                prunedSize = size * evictionPercentage / ONE_HUNDRED_PERCENT + 1;
                removalSize = Math.max(diffFromTargetSize, prunedSize);
                break;
            case USED_HEAP_PERCENTAGE:
            case USED_HEAP_SIZE:
            case FREE_HEAP_PERCENTAGE:
            case FREE_HEAP_SIZE:
                // if we have an evictable size, be sure to evict at least one entry in worst case.
                removalSize = Math.max(size * evictionPercentage / ONE_HUNDRED_PERCENT, 1);
                break;
            default:
                throw new IllegalArgumentException("Max size policy is not defined [" + maxSizePolicy + "]");
        }
        return removalSize;
    }

    protected long getEvictionCriteriaValue(Record record, EvictionPolicy evictionPolicy) {
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


    @Override
    public boolean isReachedMaxSize(RecordStore recordStore) {
        if (recordStore.size() == 0) {
            return false;
        }
        String mapName = recordStore.getName();
        int partitionId = recordStore.getPartitionId();

        MapContainer mapContainer = recordStore.getMapContainer();
        MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();

        boolean result;
        switch (maxSizePolicy) {
            case PER_NODE:
                return checkPerNodeEviction(mapName, maxSizeConfig);
            case PER_PARTITION:
                result = checkPerPartitionEviction(mapName, maxSizeConfig, partitionId);
                break;
            case USED_HEAP_PERCENTAGE:
                result = checkHeapPercentageEviction(mapName, maxSizeConfig);
                break;
            case USED_HEAP_SIZE:
                result = checkHeapSizeEviction(mapName, maxSizeConfig);
                break;
            case FREE_HEAP_PERCENTAGE:
                result = checkFreeHeapPercentageEviction(maxSizeConfig);
                break;
            case FREE_HEAP_SIZE:
                result = checkFreeHeapSizeEviction(maxSizeConfig);
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate max size policy [" + maxSizePolicy + ']');
        }
        return result;
    }


    protected boolean checkPerNodeEviction(String mapName, MaxSizeConfig maxSizeConfig) {
        long nodeTotalSize = 0;

        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
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

    protected boolean checkPerPartitionEviction(String mapName, MaxSizeConfig maxSizeConfig, int partitionId) {
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        if (container == null) {
            return false;
        }
        final int size = getRecordStoreSize(mapName, container);
        return size >= maxSize;
    }

    protected boolean checkHeapSizeEviction(String mapName, MaxSizeConfig maxSizeConfig) {
        final long usedHeapSize = getUsedHeapSize(mapName);
        if (usedHeapSize == -1L) {
            return false;
        }
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return maxSize < (1D * usedHeapSize / ONE_MEGABYTE);
    }

    protected boolean checkFreeHeapSizeEviction(MaxSizeConfig maxSizeConfig) {
        final long currentFreeHeapSize = getAvailableMemory();
        final double minFreeHeapSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return minFreeHeapSize > (1D * currentFreeHeapSize / ONE_MEGABYTE);
    }

    protected boolean checkHeapPercentageEviction(String mapName, MaxSizeConfig maxSizeConfig) {
        final long usedHeapSize = getUsedHeapSize(mapName);
        if (usedHeapSize == -1L) {
            return false;
        }
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = getTotalMemory();
        return maxSize < (1D * ONE_HUNDRED_PERCENT * usedHeapSize / total);
    }

    protected boolean checkFreeHeapPercentageEviction(MaxSizeConfig maxSizeConfig) {
        final long currentFreeHeapSize = getAvailableMemory();
        final double freeHeapPercentage = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = getTotalMemory();
        return freeHeapPercentage > (1D * ONE_HUNDRED_PERCENT * currentFreeHeapSize / total);
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

    protected long getUsedHeapSize(String mapName) {
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
        heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
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
        return existingRecordStore.getHeapCost();
    }

    /**
     * used when deciding evictable or not.
     */
    protected static double getApproximateMaxSize(int maxSizeFromConfig) {
        // because not to exceed the max size much we start eviction early.
        // so decrease the max size with ratio .95 below
        return 1D * maxSizeFromConfig * EVICTION_START_THRESHOLD_PERCENTAGE / ONE_HUNDRED_PERCENT;
    }

    /**
     * Get max size setting form config for given policy
     *
     * @return max size or -1 if policy is different or not set
     */
    public static double getApproximateMaxSize(MaxSizeConfig maxSizeConfig, MaxSizeConfig.MaxSizePolicy policy) {
        if (maxSizeConfig.getMaxSizePolicy() == policy) {
            return getApproximateMaxSize(maxSizeConfig.getSize());
        }
        return -1D;
    }

    protected List<Integer> findPartitionIds() {
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


    protected boolean isOwnerOrBackup(int partitionId) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final InternalPartition partition = partitionService.getPartition(partitionId, false);
        final Address thisAddress = nodeEngine.getThisAddress();
        return partition.isOwnerOrBackup(thisAddress);
    }

    // only used when testing.
    public void setMemoryInfoAccessor(MemoryInfoAccessor memoryInfoAccessor) {
        this.memoryInfoAccessor = memoryInfoAccessor;
    }


}

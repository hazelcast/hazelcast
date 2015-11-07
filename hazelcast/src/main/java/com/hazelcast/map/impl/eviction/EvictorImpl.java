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
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.util.Clock;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Eviction helper methods.
 */
public class EvictorImpl implements Evictor {

    protected static final int ONE_HUNDRED_PERCENT = 100;

    protected final MapServiceContext mapServiceContext;
    protected final EvictionChecker evictionChecker;

    public EvictorImpl(EvictionChecker evictionChecker, MapServiceContext mapServiceContext) {
        this.evictionChecker = evictionChecker;
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public EvictionChecker getEvictionChecker() {
        return evictionChecker;
    }

    @Override
    public void removeSize(int removalSize, RecordStore recordStore) {
        long now = Clock.currentTimeMillis();
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();

        boolean backup = isBackup(recordStore);

        final EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();
        // criteria is a long value, like last access times or hits,
        // used for calculating LFU or LRU.
        final long[] criterias = createAndPopulateEvictionCriteriaArray(recordStore, evictionPolicy);
        if (criterias == null) {
            return;
        }
        Arrays.sort(criterias);
        // check in case record store size may be smaller than evictable size.
        final int evictableBaseIndex = getEvictionStartIndex(criterias, removalSize);
        final long criteriaValue = criterias[evictableBaseIndex];
        int evictedRecordCounter = 0;
        final Iterator<Record> iterator = recordStore.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            long value = getEvictionCriteriaValue(record, evictionPolicy);
            if (value <= criteriaValue) {
                if (tryEvict(key, record, recordStore, backup, now)) {
                    evictedRecordCounter++;
                }
            }
            if (evictedRecordCounter >= removalSize) {
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

    @Override
    public int findRemovalSize(RecordStore recordStore) {
        MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
        int maxSize = mapConfig.getMaxSizeConfig().getSize();
        int currentRecordStoreSize = recordStore.size();

        MaxSizeConfig.MaxSizePolicy maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
        int evictionPercentage = mapConfig.getEvictionPercentage();

        switch (maxSizePolicy) {
            case PER_PARTITION:
                double maxExpectedRecordStoreSize = Double.valueOf(maxSize
                        * ((ONE_HUNDRED_PERCENT - evictionPercentage) / (1D * ONE_HUNDRED_PERCENT))).intValue();
                double diffFromTargetSize = currentRecordStoreSize - maxExpectedRecordStoreSize;
                int prunedSize = currentRecordStoreSize * evictionPercentage / ONE_HUNDRED_PERCENT + 1;
                return Math.max((int) diffFromTargetSize, prunedSize);
            case PER_NODE:
                maxExpectedRecordStoreSize
                        = ((EvictionCheckerImpl) evictionChecker).calculatePerNodeMaxRecordStoreSize(recordStore);
                int expectedSizeAfterEviction = (int) (maxExpectedRecordStoreSize
                        * (ONE_HUNDRED_PERCENT - evictionPercentage) / ONE_HUNDRED_PERCENT);
                expectedSizeAfterEviction = Math.max(expectedSizeAfterEviction, 1);
                return currentRecordStoreSize - expectedSizeAfterEviction;
            case USED_HEAP_PERCENTAGE:
            case USED_HEAP_SIZE:
            case FREE_HEAP_PERCENTAGE:
            case FREE_HEAP_SIZE:
                // if we have an evictable size, be sure to evict at least one entry in worst case.
                return Math.max(currentRecordStoreSize * evictionPercentage / ONE_HUNDRED_PERCENT, 1);
            default:
                throw new IllegalArgumentException("Max size policy is not defined [" + maxSizePolicy + "]");
        }
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

}

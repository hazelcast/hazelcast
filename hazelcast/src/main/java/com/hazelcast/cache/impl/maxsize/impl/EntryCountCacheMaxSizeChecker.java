package com.hazelcast.cache.impl.maxsize.impl;

import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.CacheMaxSizeConfig;

/**
 * Cache max-size policy implementation for
 * {@link com.hazelcast.config.CacheMaxSizeConfig.CacheMaxSizePolicy#ENTRY_COUNT}
 */
public class EntryCountCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final CacheRecordMap cacheRecordMap;
    private final int maxPartitionSize;

    public EntryCountCacheMaxSizeChecker(CacheMaxSizeConfig maxSizeConfig,
            CacheRecordMap cacheRecordMap, int partitionCount) {
        this.cacheRecordMap = cacheRecordMap;
        this.maxPartitionSize = calculateMaxPartitionSize(maxSizeConfig.getSize(), partitionCount);
    }

    //CHECKSTYLE:OFF
    protected int calculateMaxPartitionSize(int maxEntryCount, int partitionCount) {
        final double balancedPartitionSize = (double) maxEntryCount / (double) partitionCount;
        final double approximatedStdDev = Math.sqrt(balancedPartitionSize);
        final int stdDevMultiplier = maxEntryCount <= 4000 ? 5 : 3;
        return (int) ((approximatedStdDev * stdDevMultiplier) + balancedPartitionSize);
    }
    //CHECKSTYLE:ON

    @Override
    public boolean isReachedToMaxSize() {
        return cacheRecordMap.size() >= maxPartitionSize;
    }

}

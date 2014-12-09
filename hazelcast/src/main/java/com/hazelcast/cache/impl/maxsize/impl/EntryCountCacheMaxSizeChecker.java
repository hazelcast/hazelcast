package com.hazelcast.cache.impl.maxsize.impl;

import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.cache.impl.record.CacheRecordMap;

/**
 * Cache max-size policy implementation for
 * {@link com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy#ENTRY_COUNT}
 */
public class EntryCountCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final CacheRecordMap cacheRecordMap;
    private final int maxPartitionSize;

    public EntryCountCacheMaxSizeChecker(final int size,
            final CacheRecordMap cacheRecordMap, int partitionCount) {
        this.cacheRecordMap = cacheRecordMap;
        this.maxPartitionSize = calculateMaxPartitionSize(size, partitionCount);
    }

    // Defined as "public static" since external tests can test the logic without knowing the logic
    // for calculating the estimated max size.
    //CHECKSTYLE:OFF
    public static int calculateMaxPartitionSize(int maxEntryCount, int partitionCount) {
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

package com.hazelcast.cache.impl.maxsize.impl;

import com.hazelcast.cache.impl.maxsize.CacheMaxSizeChecker;
import com.hazelcast.cache.impl.record.CacheRecordMap;

/**
 * Cache max-size policy implementation for
 * {@link com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy#ENTRY_COUNT}
 */
public class EntryCountCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private static final int MAX_ENTRY_COUNT_FOR_THRESHOLD_USAGE = 1000000;
    private static final int STD_DEV_OF_5_THRESHOLD = 4000;
    private static final int STD_DEV_MULTIPLIER_5 = 5;
    private static final int STD_DEV_MULTIPLIER_3 = 3;

    private final CacheRecordMap cacheRecordMap;
    private final int maxPartitionSize;

    public EntryCountCacheMaxSizeChecker(final int size,
            final CacheRecordMap cacheRecordMap, final int partitionCount) {
        this.cacheRecordMap = cacheRecordMap;
        this.maxPartitionSize = calculateMaxPartitionSize(size, partitionCount);
    }

    // Defined as "public static" since external tests can test the logic without knowing the logic
    // for calculating the estimated max size.
    public static int calculateMaxPartitionSize(int maxEntryCount, int partitionCount) {
        final double balancedPartitionSize = (double) maxEntryCount / (double) partitionCount;
        final double approximatedStdDev = Math.sqrt(balancedPartitionSize);
        int stdDevMultiplier;

        if (maxEntryCount <= STD_DEV_OF_5_THRESHOLD) {
            // Below 4.000 entry count, multiplying standard deviation with 5 gives better estimations
            // as our experiments and test results.
            stdDevMultiplier = STD_DEV_MULTIPLIER_5;
        } else if (maxEntryCount > STD_DEV_OF_5_THRESHOLD
                && maxEntryCount <= MAX_ENTRY_COUNT_FOR_THRESHOLD_USAGE) {
            // Above 4.000 entry count but below 1.000.000 entry count,
            // multiplying standard deviation with 3 gives better estimations
            // as our experiments and test results.
            stdDevMultiplier = STD_DEV_MULTIPLIER_3;
        } else {
            // Over 1.000.000 entries, there is no need for using standard deviation
            stdDevMultiplier = 0;
        }
        return (int) ((approximatedStdDev * stdDevMultiplier) + balancedPartitionSize);
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheRecordMap.size() >= maxPartitionSize;
    }

}

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.maxsize.impl;

import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.eviction.EvictionChecker;

/**
 * Cache max-size policy implementation for
 * {@link MaxSizePolicy#ENTRY_COUNT}
 */
public class EntryCountCacheEvictionChecker
        implements EvictionChecker {

    private static final int MAX_ENTRY_COUNT_FOR_THRESHOLD_USAGE = 1000000;
    private static final int STD_DEV_OF_5_THRESHOLD = 4000;
    private static final int STD_DEV_MULTIPLIER_5 = 5;
    private static final int STD_DEV_MULTIPLIER_3 = 3;

    private final CacheRecordMap cacheRecordMap;
    private final int maxPartitionSize;

    public EntryCountCacheEvictionChecker(final int size,
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
    public boolean isEvictionRequired() {
        return cacheRecordMap.size() >= maxPartitionSize;
    }

}

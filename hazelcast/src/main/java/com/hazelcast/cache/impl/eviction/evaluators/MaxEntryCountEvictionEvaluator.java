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

package com.hazelcast.cache.impl.eviction.evaluators;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictableStore;
import com.hazelcast.cache.impl.eviction.EvictionEvaluator;

/**
 * This {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy} implementation examines the
 * maximum number of allowed elements in a partition and makes decisions based on the currently stored
 * element count.<br>
 * Up to 4000 elements we apply a standard deviation of
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 * @param <S> type of the {@link com.hazelcast.cache.impl.eviction.EvictableStore} that stores the entries
 */
public class MaxEntryCountEvictionEvaluator<A, E extends Evictable, S extends EvictableStore<A, E>>
        implements EvictionEvaluator<A, E, S> {

    public static final int STD_DEV_OF_5_THRESHOLD = 4000;
    public static final int STD_DEV_MULTIPLIER_5 = 5;
    public static final int STD_DEV_MULTIPLIER_3 = 3;

    private static final int[][] STD_DEV = {
            {10, 1}, {100, 1}, {500, 2}, {1000, 3}, {5000, 5},
            {10000, 6}, {100000, 21}, {1000000, 60}, };

    private final int partitionCount;

    private MaxSizeRule<A, E, S> maxSizeRule;

    public MaxEntryCountEvictionEvaluator(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    public boolean isEvictionRequired(S evictableStore) {
        MaxSizeRule<A, E, S> maxSizeRule = getMaxSizeRule(evictableStore);
        return maxSizeRule.apply(evictableStore);
    }

    private MaxSizeRule<A, E, S> getMaxSizeRule(S evictableStore) {
        if (maxSizeRule != null) {
            return maxSizeRule;
        }

        long globalCapacity = evictableStore.getGlobalEntryCapacity();
        long balancedPartitionSize = (long) ((double) globalCapacity / partitionCount);

        MaxSizeRule<A, E, S> maxSizeRule;
        if (globalCapacity <= STD_DEV_OF_5_THRESHOLD) {
            maxSizeRule = stdDev(evictableStore, balancedPartitionSize, STD_DEV_MULTIPLIER_5);
        } else {
            maxSizeRule = stdDev(evictableStore, balancedPartitionSize, STD_DEV_MULTIPLIER_3);
        }

        this.maxSizeRule = maxSizeRule;
        return maxSizeRule;
    }

    private interface MaxSizeRule<A, E extends Evictable, S extends EvictableStore<A, E>> {
        boolean apply(S evictableStore);
    }

    private MaxSizeRule<A, E, S> staticMaxSizeRule(final long maxPartitionSize) {
        return new MaxSizeRule<A, E, S>() {
            @Override
            public boolean apply(S evictableStore) {
                long partitionSize = evictableStore.getPartitionEntryCount();
                return partitionSize >= maxPartitionSize;
            }
        };
    }

    private MaxSizeRule<A, E, S> estimationMaxSizeRule() {
        return new MaxSizeRule<A, E, S>() {
            @Override
            public boolean apply(S evictableStore) {
                long partitionSize = evictableStore.getPartitionEntryCount();
                long globalCapacity = evictableStore.getGlobalEntryCapacity();
                return (partitionSize * partitionSize) >= globalCapacity;
            }
        };
    }

    private MaxSizeRule<A, E, S> stdDev(final S evictableStore, final long balancedPartitionSize, final int multiplier) {
        long globalCapacity = evictableStore.getGlobalEntryCapacity();

        long stdDev = 0;
        for (int[] temp : STD_DEV) {
            if (temp[0] <= globalCapacity) {
                stdDev = temp[1];
            }
        }

        final long maxPartitionSize = stdDev * multiplier + balancedPartitionSize;
        return new MaxSizeRule<A, E, S>() {
            @Override
            public boolean apply(S evictableStore) {
                long partitionSize = evictableStore.getPartitionEntryCount();
                return partitionSize >= maxPartitionSize;
            }
        };
    }
}

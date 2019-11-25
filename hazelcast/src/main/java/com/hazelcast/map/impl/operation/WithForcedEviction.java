/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.memory.NativeOutOfMemoryError;

import static java.lang.Integer.getInteger;

/**
 * This file contains retry logic of a {@link MapOperation}
 * in the event of {@link NativeOutOfMemoryError}. This logic
 * only works with maps which have NATIVE in memory format.
 * <p>
 * If an {@code IMap} is evictable, naturally expected
 * thing is success of all put operations. Because if
 * there is not enough memory, eviction kicks in and
 * empties a portion of memory in order to put new entries.
 * <p>
 * Here we do evictions forcibly by touching all evictable
 * record-stores in a partition, at worst we could delete all
 * entries from all evictable IMaps which fell to this partition.
 */
public final class WithForcedEviction {

    public static final int DEFAULT_FORCED_EVICTION_RETRY_COUNT = 5;
    public static final String PROP_FORCED_EVICTION_RETRY_COUNT
            = "hazelcast.internal.forced.eviction.retry.count";

    static final int EVICTION_RETRY_COUNT = getInteger(PROP_FORCED_EVICTION_RETRY_COUNT,
            DEFAULT_FORCED_EVICTION_RETRY_COUNT);

    private static final double TWENTY_PERCENT = 0.2D;
    private static final double HUNDRED_PERCENT = 1D;
    private static final double[] EVICTION_PERCENTAGES = {TWENTY_PERCENT, HUNDRED_PERCENT};
    private static final ForcedEviction[] EVICTION_STRATEGIES
            = {new SingleRecordStoreForcedEviction(), new MultipleRecordStoreForcedEviction()};


    private WithForcedEviction() {
    }

    public static void runWithForcedEvictionStrategies(MapOperation operation) {
        for (double evictionPercentage : EVICTION_PERCENTAGES) {
            for (ForcedEviction evictionStrategy : EVICTION_STRATEGIES) {
                if (evictionStrategy.forceEvictAndRun(operation, evictionPercentage)) {
                    return;
                }
            }
        }
    }
}

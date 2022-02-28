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

package com.hazelcast.query;

import com.hazelcast.instance.LocalInstanceStats;
import com.hazelcast.internal.monitor.MemberState;
import com.hazelcast.map.LocalMapStats;

/**
 * Provides local statistics for an index to be used by {@link MemberState}
 * implementations.
 */
public interface LocalIndexStats extends LocalInstanceStats {

    /**
     * Returns the total number of queries served by the index.
     * <p>
     * To calculate the index hit rate just divide the returned value by a value
     * returned by {@link LocalMapStats#getQueryCount()}.
     * <p>
     * The returned value may be less than the one returned by {@link
     * #getHitCount()} since a single query may hit the same index more than once.
     */
    long getQueryCount();

    /**
     * Returns the total number of hits into the index.
     * <p>
     * The returned value may be greater than the one returned by {@link
     * #getQueryCount} since a single query may hit the same index more than once.
     */
    long getHitCount();

    /**
     * Returns the average hit latency (in nanoseconds) for the index.
     */
    long getAverageHitLatency();

    /**
     * Returns the average selectivity of the hits served by the index.
     * <p>
     * The returned value is in the range from 0.0 to 1.0. Values close to 1.0
     * indicate a high selectivity meaning the index is efficient; values close
     * to 0.0 indicate a low selectivity meaning the index efficiency is
     * approaching an efficiency of a simple full scan.
     */
    double getAverageHitSelectivity();

    /**
     * Returns the number of insert operations performed on the index.
     */
    long getInsertCount();

    /**
     * Returns the total latency (in nanoseconds) of insert operations performed
     * on the index.
     * <p>
     * To compute the average latency divide the returned value by {@link
     * #getInsertCount() insert operation count}.
     */
    long getTotalInsertLatency();

    /**
     * Returns the number of update operations performed on the index.
     */
    long getUpdateCount();

    /**
     * Returns the total latency (in nanoseconds) of update operations performed
     * on the index.
     * <p>
     * To compute the average latency divide the returned value by {@link
     * #getUpdateCount() update operation count}.
     */
    long getTotalUpdateLatency();

    /**
     * Returns the number of remove operations performed on the index.
     */
    long getRemoveCount();

    /**
     * Returns the total latency (in nanoseconds) of remove operations performed
     * on the index.
     * <p>
     * To compute the average latency divide the returned value by {@link
     * #getRemoveCount() remove operation count}.
     */
    long getTotalRemoveLatency();

    /**
     * Returns the memory cost of the index in bytes.
     * <p>
     * Currently, for on-heap indexes (OBJECT and BINARY storages), the returned
     * value is just a best-effort approximation and doesn't indicate a precise
     * on-heap memory usage of the index.
     */
    long getMemoryCost();

}

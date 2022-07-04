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

package com.hazelcast.internal.monitor;

import com.hazelcast.instance.LocalInstanceStats;

/**
 * Local cache statistics to be used by {@link MemberState} implementations.
 */
public interface LocalCacheStats extends LocalInstanceStats {

    /**
     * Gets the last access time to cache.
     *
     * @return the last access time to cache
     */
    long getLastAccessTime();

    /**
     * Gets the last update time to cache.
     *
     * @return the last update time to cache
     */
    long getLastUpdateTime();

    /**
     * Returns the owned entry count in the cache.
     *
     * @return the owned entry count in the cache
     */
    long getOwnedEntryCount();

    /**
     * Returns the number of hits (successful get operations) on the cache.
     *
     * @return the number of hits (successful get operations) on the cache
     */
    long getCacheHits();

    /**
     * Returns the percentage of hits (successful get operations) on the cache.
     *
     * @return the percentage of hits (successful get operations) on the cache
     */
    float getCacheHitPercentage();

    /**
     * Returns the number of missed cache accesses on the cache.
     *
     * @return the number of missed cache accesses on the cache
     */
    long getCacheMisses();

    /**
     * Returns the percentage of missed cache accesses on the cache.
     *
     * @return the percentage of missed cache accesses on the cache
     */
    float getCacheMissPercentage();

    /**
     * Returns the number of gets on the cache.
     *
     * @return the number of gets on the cache
     */
    long getCacheGets();

    /**
     * Returns the number of puts to the queue.
     *
     * @return the number of puts to the queue
     */
    long getCachePuts();

    /**
     * Returns the number of removals from the queue.
     *
     * @return the number of removals from the queue
     */
    long getCacheRemovals();

    /**
     * Returns the number of evictions on the cache.
     *
     * @return the number of evictions on the cache
     */
    long getCacheEvictions();

    /**
     * Returns the mean time to execute gets on the cache.
     *
     * @return the mean time in µs to execute gets on the cache
     */
    float getAverageGetTime();

    /**
     * Returns the mean time to execute puts on the cache.
     *
     * @return the mean time in µs to execute puts on the cache
     */
    float getAveragePutTime();

    /**
     * Returns the mean time to execute removes on the cache.
     *
     * @return the mean time in µs to execute removes on the cache
     */
    float getAverageRemoveTime();
}

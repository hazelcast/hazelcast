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

package com.hazelcast.cache;

import com.hazelcast.partition.LocalReplicationStats;
import com.hazelcast.nearcache.NearCacheStats;

/**
 * Cache statistics
 * <p>
 *     Cache statistics are accumulated starting from the time a cache is created. <br>
 *     An instance of this class represents local node values only! For an accumulated view
 *     on cluster level, the user has to retrieve all nodes statistics and aggregate values
 *     on his own.
 * </p>
 * <p>
 *     Sample code:
 *     <pre>
 *       ICache&lt;Key, Value&gt; unwrappedCache =  cache.unwrap( ICache.class );
 *       CacheStatistics cacheStatistics = unwrappedCache.getLocalCacheStatistics();
 *       long cacheHits = cacheStatistics.getCacheHits();
 *     </pre>
 *
 * @since 3.3.1
 */
public interface CacheStatistics {

    /**
     * Gets the cache creation time.
     *
     * @return the cache creation time
     */
    long getCreationTime();

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
     * The number of get requests that were satisfied by the cache.
     * <p>
     * {@link javax.cache.Cache#containsKey(Object)} is not a get request for
     * statistics purposes.
     * <p>
     * In caches with multiple tiered storage, a hit may be implemented as a hit
     * to the cache or to the first tier.
     * <p>
     * For an {@link javax.cache.processor.EntryProcessor}, a hit occurs when the
     * key exists and an entry processor can be invoked against it, even if no
     * methods of {@link javax.cache.Cache.Entry} or
     * {@link javax.cache.processor.MutableEntry} are called.
     *
     * @return the number of hits (get requests that were satisfied by the cache)
     */
    long getCacheHits();

    /**
     * This is a measure of cache efficiency.
     * <p>
     * It is calculated as:
     * {@link #getCacheHits} divided by {@link #getCacheGets ()} * 100.
     *
     * @return the percentage of successful hits as a decimal, such as 75
     */
    float getCacheHitPercentage();

    /**
     * A miss is a get request that is not satisfied.
     * <p>
     * In a simple cache, a miss occurs when the cache does not satisfy the request.
     * <p>
     * {@link javax.cache.Cache#containsKey(Object)} is not a get request for
     * statistics purposes.
     * <p>
     * For an {@link javax.cache.processor.EntryProcessor}, a miss occurs when the
     * key does not exist and therefore an entry processor cannot be invoked
     * against it.
     * <p>
     * In caches with multiple tiered storage, a miss may be implemented as a miss
     * to the cache or to the first tier.
     * <p>
     * In a read-through cache, a miss is an absence of the key in the cache that
     * will trigger a call to a CacheLoader. So it is still a miss even though the
     * cache will load and return the value.
     * <p>
     * Refer to the implementation for precise semantics.
     *
     * @return the number of misses (get requests that were not satisfied)
     */
    long getCacheMisses();

    /**
     * Returns the percentage of cache accesses that did not find a requested entry
     * in the cache.
     * <p>
     * This is calculated as {@link #getCacheMisses()} divided by
     * {@link #getCacheGets()} * 100.
     *
     * @return the percentage of cache accesses that failed to find anything
     */
    float getCacheMissPercentage();

    /**
     * The total number of requests to the cache. This will be equal to the sum of
     * the hits and misses.
     * <p>
     * A "get" is an operation that returns the current or previous value. It does
     * not include checking for the existence of a key.
     * <p>
     * In caches with multiple tiered storage, a get may be implemented as a get
     * to the cache or to the first tier.
     *
     * @return the number of gets to the cache
     */
    long getCacheGets();

    /**
     * The total number of puts to the cache.
     * <p>
     * A put is counted even if it is immediately evicted.
     * <p>
     * Replaces are where a put occurs which overrides an existing mapping, and they are counted
     * as a put.
     *
     * @return the number of puts to the cache
     */
    long getCachePuts();

    /**
     * The total number of removals from the cache. This does not include evictions,
     * where the cache itself initiates the removal to make space.
     *
     * @return the number of removals from the cache
     */
    long getCacheRemovals();

    /**
     * The total number of evictions from the cache. An eviction is a removal
     * initiated by the cache itself to free up space. An eviction is not treated as
     * a removal and does not appear in the removal counts.
     *
     * @return the total number of evictions from the cache
     */
    long getCacheEvictions();

    /**
     * The mean time to execute gets.
     * <p>
     * In a read-through cache, the time taken to load an entry on miss is not
     * included in get time.
     *
     * @return the mean time in microseconds to execute gets
     */
    float getAverageGetTime();

    /**
     * The mean time to execute puts.
     *
     * @return the mean time in microseconds to execute puts
     */
    float getAveragePutTime();

    /**
     * The mean time to execute removes.
     *
     * @return the mean time in microseconds to execute removes
     */
    float getAverageRemoveTime();

    /**
     * Gets the Near Cache statistics.
     *
     * @return the Near Cache statistics
     */
    NearCacheStats getNearCacheStatistics();

    /**
     * @return replication statistics.
     * @since 5.0
     */
    LocalReplicationStats getReplicationStats();
}

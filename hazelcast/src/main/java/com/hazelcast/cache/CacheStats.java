package com.hazelcast.cache;

import com.hazelcast.monitor.NearCacheStats;

/**
 * Statistics for the {@link com.hazelcast.cache.ICache}
 */
public interface CacheStats {

    /**
     * Returns the total hit count
     *
     * @return hit count
     */
    long getHits();

    /**
     * Returns the total miss count
     *
     * @return miss count
     */
    long getMisses();

    /**
     * Returns the total put count
     *
     * @return put count
     */
    long getPuts();

    /**
     * Returns the total get count
     *
     * @return get count
     */
    long getGets();

    /**
     * Returns the total remove count
     *
     * @return remove count
     */
    long getRemoves();

    /**
     * Returns the average latency for put operations in milliseconds
     *
     * @return average put latency
     */
    double getAveragePutLatency();

    /**
     * Returns the average latency for get operations in milliseconds
     *
     * @return average get latency
     */
    double getAverageGetLatency();

    /**
     * Returns the average latency for remove operations in milliseconds
     *
     * @return average remove latency
     */
    double getAverageRemoveLatency();

    /**
     * Returns the cache's creation time
     *
     * @return creation time
     */
    long getCreationTime();

    /**
     * Returns the time when the cache is updated last
     *
     * @return last update time
     */
    long getLastUpdateTime();

    /**
     * Returns the time when the cache is accessed last
     *
     * @return last access time
     */
    long getLastAccessTime();

    /**
     * Returns the near cache stats
     *
     * @return near cache stats
     */
    NearCacheStats getNearCacheStats();
}

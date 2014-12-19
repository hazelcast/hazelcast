package com.hazelcast.monitor;

/**
 * Local cache statistics interface to be used
 * by {@link com.hazelcast.monitor.MemberState} implementations.
 */
public interface LocalCacheStats extends LocalInstanceStats {

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

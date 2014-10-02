package com.hazelcast.monitor;

/**
 * Local cache statistics interface to be used
 * by {@link com.hazelcast.monitor.MemberState} implementations.
 */
public interface LocalCacheStats extends LocalInstanceStats {

    /**
     *
     * @return the number of hits
     */
    long getCacheHits();

    /**
     *
     * @return the percentage of hits (successful get operations)
     */
    float getCacheHitPercentage();

    /**
     *
     * @return the number misses
     */
    long getCacheMisses();

    /**
     *
     * @return the percentage of missed cache accesses
     */
    float getCacheMissPercentage();

    /**
     *
     * @return the number of gets
     */
    long getCacheGets();

    /**
     *
     * @return the number of puts
     */
    long getCachePuts();

    /**
     *
     * @return the number of removals
     */
    long getCacheRemovals();

    /**
     *
     * @return the number of evictions
     */
    long getCacheEvictions();

    /**
     * The mean time to execute gets
     *
     * @return the time in µs
     */
    float getAverageGetTime();

    /**
     * The mean time to execute puts
     *
     * @return the time in µs
     */
    float getAveragePutTime();

    /**
     * The mean time to execute removes
     *
     * @return the time in µs
     */
    float getAverageRemoveTime();

}

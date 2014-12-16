package com.hazelcast.cache.impl.maxsize;

/**
 * Interface for implementations of {@link com.hazelcast.config.CacheMaxSizeConfig.CacheMaxSizePolicy}.
 */
public interface CacheMaxSizeChecker {

    /**
     * Checks the state of cache to see if it has reached its maximum configured size
     * {@link com.hazelcast.config.CacheMaxSizeConfig.CacheMaxSizePolicy}
     *
     * @return true if cache has reached maximum size, false otherwise
     */
    boolean isReachedToMaxSize();

}

package com.hazelcast.cache.impl.maxsize;

/**
 * Interface for implementations of {@link com.hazelcast.config.CacheMaxSizeConfig.CacheMaxSizePolicy}.
 */
public interface CacheMaxSizeChecker {

    /**
     * Checks the state of cache about if is reached to max size as its configured
     * {@link com.hazelcast.config.CacheMaxSizeConfig.CacheMaxSizePolicy}
     *
     * @return <code>true</code> if cache is reached to max-size, otherwise <code>false</code>
     */
    boolean isReachedToMaxSize();

}

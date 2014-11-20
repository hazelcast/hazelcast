package com.hazelcast.cache.impl.maxsize;

import com.hazelcast.cache.impl.CacheInfo;
import com.hazelcast.config.MaxSizeConfig;

/**
 * @author sozal 20/11/14
 */
public class PerNodeCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final CacheInfo cacheInfo;
    private final int maxEntryCount;

    public PerNodeCacheMaxSizeChecker(CacheInfo cacheInfo, MaxSizeConfig maxSizeConfig) {
        this.cacheInfo = cacheInfo;
        this.maxEntryCount = maxSizeConfig.getSize();
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheInfo.getEntryCount() >= maxEntryCount;
    }

}

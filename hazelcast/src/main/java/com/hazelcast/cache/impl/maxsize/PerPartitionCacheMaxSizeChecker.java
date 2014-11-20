package com.hazelcast.cache.impl.maxsize;

import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.MaxSizeConfig;

/**
 * @author sozal 20/11/14
 */
public class PerPartitionCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private final CacheRecordMap cacheRecordMap;
    private final int maxEntryCount;

    public PerPartitionCacheMaxSizeChecker(CacheRecordMap cacheRecordMap, MaxSizeConfig maxSizeConfig) {
        this.cacheRecordMap = cacheRecordMap;
        this.maxEntryCount = maxSizeConfig.getSize();
    }

    @Override
    public boolean isReachedToMaxSize() {
        return cacheRecordMap.size() >= maxEntryCount;
    }

}

package com.hazelcast.cache.impl.maxsize;

import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.CacheMaxSizeConfig;

/**
 * Cache max-size policy implementation for
 * {@link com.hazelcast.config.CacheMaxSizeConfig.CacheMaxSizePolicy#ENTRY_COUNT}
 *
 * @author sozal 20/11/14
 */
public class EntryCountCacheMaxSizeChecker implements CacheMaxSizeChecker {

    private static final float ENTRY_COUNT_FACTOR = 1.0F;

    private final int maxEntryCount;
    private final CacheRecordMap cacheRecordMap;
    private final int partitionCount;

    public EntryCountCacheMaxSizeChecker(CacheMaxSizeConfig maxSizeConfig,
            CacheRecordMap cacheRecordMap, int partitionCount) {
        this.maxEntryCount = maxSizeConfig.getSize();
        this.cacheRecordMap = cacheRecordMap;
        this.partitionCount = partitionCount;
    }

    @Override
    public boolean isReachedToMaxSize() {
        final int estimatedSize = (int) ((cacheRecordMap.size() * partitionCount) * ENTRY_COUNT_FACTOR);
        return estimatedSize >= maxEntryCount;
    }

}

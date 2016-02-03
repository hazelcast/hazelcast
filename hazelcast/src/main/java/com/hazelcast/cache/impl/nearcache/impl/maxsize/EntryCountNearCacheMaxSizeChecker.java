/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache.impl.maxsize;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.cache.impl.nearcache.impl.NearCacheRecordMap;

/**
 * Near-Cache max-size policy implementation for
 * {@link com.hazelcast.config.EvictionConfig.MaxSizePolicy#ENTRY_COUNT}.
 * Check if near-cache size is reached to max-size or not.
 *
 * @see com.hazelcast.cache.impl.maxsize.MaxSizeChecker
 */
public class EntryCountNearCacheMaxSizeChecker implements MaxSizeChecker {

    private final NearCacheRecordMap nearCacheRecordMap;
    private final int maxSize;

    public EntryCountNearCacheMaxSizeChecker(final int size,
                                             final NearCacheRecordMap nearCacheRecordMap) {
        this.maxSize = size;
        this.nearCacheRecordMap = nearCacheRecordMap;
    }

    @Override
    public boolean isReachedToMaxSize() {
        return nearCacheRecordMap.size() >= maxSize;
    }

}

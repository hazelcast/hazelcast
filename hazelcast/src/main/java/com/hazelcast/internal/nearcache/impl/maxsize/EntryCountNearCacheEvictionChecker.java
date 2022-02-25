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

package com.hazelcast.internal.nearcache.impl.maxsize;

import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;

/**
 * Near Cache max-size policy implementation for {@link MaxSizePolicy#ENTRY_COUNT}.
 * <p>
 * Checks if the Near Cache size is reached to max-size or not.
 *
 * @see EvictionChecker
 */
public class EntryCountNearCacheEvictionChecker
        implements EvictionChecker {

    private final SampleableNearCacheRecordMap nearCacheRecordMap;
    private final int maxSize;

    public EntryCountNearCacheEvictionChecker(final int size,
                                              final SampleableNearCacheRecordMap nearCacheRecordMap) {
        this.maxSize = size;
        this.nearCacheRecordMap = nearCacheRecordMap;
    }

    @Override
    public boolean isEvictionRequired() {
        return nearCacheRecordMap.size() >= maxSize;
    }
}

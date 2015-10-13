/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;

/**
 * A class providing static factory methods that create various entry view objects.
 */
public final class CacheEntryViews {

    private CacheEntryViews() {
    }

    /**
     *
     * Creates a {@link DefaultCacheEntryView} instance.
     *
     * @param key - Key to be wrapped
     * @param value - Value to be wrapped
     * @param record - {@link CacheRecord} instance to gather additional entry view properties like access time,
     *               expiration time and access hit.
     * @return {@link DefaultCacheEntryView} instance
     */
    public static CacheEntryView<Data, Data> createDefaultEntryView(Data key, Data value, CacheRecord record) {
        DefaultCacheEntryView entryView = new DefaultCacheEntryView(key, value,
                record.getExpirationTime(), record.getAccessTime(), record.getAccessHit());
        return entryView;
    }

}

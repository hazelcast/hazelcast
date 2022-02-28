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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.merge.entry.DefaultCacheEntryView;
import com.hazelcast.cache.impl.merge.entry.LazyCacheEntryView;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * A class providing static factory methods that create various entry view objects.
 */
public final class CacheEntryViews {

    private CacheEntryViews() {
    }

    /**
     * Types of built-in {@link CacheEntryView} implementations.
     */
    public enum CacheEntryViewType {

        /**
         * Represents {@link DefaultCacheEntryView}
         */
        DEFAULT,

        /**
         * Represents {@link LazyCacheEntryView}
         */
        LAZY

    }

    /**
     * Creates a {@link DefaultCacheEntryView} instance.
     *
     * @param key       the key to be wrapped
     * @param value     the value to be wrapped
     * @param record    {@link CacheRecord} instance to gather additional entry view properties like access time,
     *                  expiration time and access hit
     * @return the {@link DefaultCacheEntryView} instance
     */
    public static CacheEntryView<Data, Data> createDefaultEntryView(Data key, Data value, Data expiryPolicy,
                                                                    CacheRecord<Object, Data> record) {
        return new DefaultCacheEntryView(key, value,
                record.getCreationTime(),
                record.getExpirationTime(),
                record.getLastAccessTime(),
                record.getHits(),
                expiryPolicy);
    }

    public static CacheEntryView<Data, Data> createEntryView(Data key, Data expiryPolicy,
                                                             CacheRecord<Object, Data> record) {
        if (record == null) {
            throw new IllegalArgumentException("Empty record");
        }
        return createDefaultEntryView(key, (Data) record.getValue(), expiryPolicy, record);
    }


    /**
     * Creates a {@link LazyCacheEntryView} instance.
     *
     * @param key       the key to be wrapped
     * @param value     the value to be wrapped
     * @param record    {@link CacheRecord} instance to gather additional entry view properties like access time,
     *                  expiration time and access hit
     * @return the {@link LazyCacheEntryView} instance
     */
    public static CacheEntryView<Data, Data> toLazyCacheEntryView(Data key, Data value, Data expiryPolicy, CacheRecord record) {
        return new LazyCacheEntryView<>(key, value,
                record.getCreationTime(),
                record.getExpirationTime(),
                record.getLastAccessTime(),
                record.getHits(),
                expiryPolicy);
    }

    public static <K, V> CacheEntryView<K, V> toLazyCacheEntryView(CacheEntryView<K, V> entryView,
                                                                   SerializationService serializationService) {
        return new LazyCacheEntryView<>(entryView.getKey(), entryView.getValue(),
                entryView.getCreationTime(),
                entryView.getExpirationTime(),
                entryView.getLastAccessTime(),
                entryView.getHits(),
                entryView.getExpiryPolicy(),
                serializationService);
    }

    /**
     * Creates a {@link CacheEntryView} instance.
     *
     * @param key                   the key to be wrapped
     * @param value                 the value to be wrapped
     * @param record                {@link CacheRecord} instance to gather additional entry view properties like
     *                              access time, expiration time and access hit
     * @param cacheEntryViewType    the type of the {@link CacheEntryView} represented as {@link CacheEntryViewType}
     * @return the {@link CacheEntryView} instance
     */
    public static CacheEntryView<Data, Data> createEntryView(Data key, Data value, Data expiryPolicy, CacheRecord record,
                                                             CacheEntryViewType cacheEntryViewType) {
        if (cacheEntryViewType == null) {
            throw new IllegalArgumentException("Empty cache entry view type");
        }
        switch (cacheEntryViewType) {
            case DEFAULT:
                return createDefaultEntryView(key, value, expiryPolicy, record);
            case LAZY:
                return toLazyCacheEntryView(key, value, expiryPolicy, record);
            default:
                throw new IllegalArgumentException("Invalid cache entry view type: " + cacheEntryViewType);
        }
    }

}

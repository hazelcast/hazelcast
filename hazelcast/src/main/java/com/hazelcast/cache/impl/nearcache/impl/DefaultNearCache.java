/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache.impl;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.SerializationService;

public class DefaultNearCache<K, V> implements NearCache<K, V> {

    protected final String name;
    protected final NearCacheConfig nearCacheConfig;
    protected final SerializationService serializationService;
    protected NearCacheRecordStore<K, V> nearCacheRecordStore;

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig,
                            NearCacheContext nearCacheContext) {
        this.name = name;
        this.nearCacheConfig = nearCacheConfig;
        this.serializationService = nearCacheContext.getSerializationService();
        this.nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, nearCacheContext);
    }

    protected NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                    NearCacheContext nearCacheContext) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == null) {
            inMemoryFormat = NearCacheConfig.DEFAULT_MEMORY_FORMAT;
        }
        switch (inMemoryFormat) {
            case BINARY:
                return new NearCacheDataRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            case OBJECT:
                return new NearCacheObjectRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            default:
                throw new IllegalArgumentException("Invalid in memory format: " + inMemoryFormat);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public V get(K key) {
        return nearCacheRecordStore.get(key);
    }

    @Override
    public void put(K key, V value) {
        nearCacheRecordStore.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        return nearCacheRecordStore.remove(key);
    }

    @Override
    public void invalidate(K key) {
        nearCacheRecordStore.remove(key);
    }

    @Override
    public boolean isInvalidateOnChange() {
        return nearCacheConfig.isInvalidateOnChange();
    }

    @Override
    public void clear() {
        nearCacheRecordStore.clear();
    }

    @Override
    public void destroy() {
        nearCacheRecordStore.destroy();
        nearCacheRecordStore = null;
    }

    @Override
    public InMemoryFormat getInMemoryFormat() {
        return nearCacheConfig.getInMemoryFormat();
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheRecordStore.getNearCacheStats();
    }

    @Override
    public Object selectToSave(Object... candidates) {
        return nearCacheRecordStore.selectToSave(candidates);
    }

}

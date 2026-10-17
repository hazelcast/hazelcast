/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.querycache.QueryCacheContext;

import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.publishEntryEvent;

/**
 * Query-cache implementation that publishes events for matching entries without keeping a local cache.
 * <p>
 * Initial population still emits {@link EntryEventType#ADDED ADDED} events for every matched entry, but the entries
 * are not stored in the query cache and are not limited by the configured query-cache entry count. Since no local
 * records are retained, remove-like events cannot include an old value unless that value is supplied by the publisher
 * event itself.
 *
 * @param <K> the key type for this {@link InternalQueryCache}
 * @param <V> the value type for this {@link InternalQueryCache}
 */
class PassThroughQueryCache<K, V> extends DefaultQueryCache<K, V> {

    PassThroughQueryCache(String cacheId, String cacheName, QueryCacheConfig queryCacheConfig,
                          IMap delegate, QueryCacheContext context) {
        super(cacheId, cacheName, queryCacheConfig, delegate, context);
    }

    @Override
    public void set(K key, V value, EntryEventType eventType) {
        if (eventType == null) {
            return;
        }

        Object queryCacheKey = recordStore.toQueryCacheKey(key);
        Data valueData = toData(value);
        publishEntryEvent(context, mapName, cacheId, queryCacheKey, valueData, null, eventType, extractors);
    }

    @Override
    public void prepopulate(Iterator<Map.Entry<Data, Data>> entries) {
        while (entries.hasNext()) {
            Map.Entry<Data, Data> entry = entries.next();
            publishEntryEvent(context, mapName, cacheId,
                    entry.getKey(), entry.getValue(), null, EntryEventType.ADDED, extractors);
        }
    }

    @Override
    public void delete(Object key, EntryEventType eventType) {
        checkNotNull(key, "key cannot be null");
        if (eventType == null) {
            return;
        }

        Object queryCacheKey = recordStore.toQueryCacheKey(key);
        publishEntryEvent(context, mapName, cacheId, queryCacheKey, null, null, eventType, extractors);
    }

    @Override
    public boolean reachedMaxCapacity() {
        return false;
    }
}

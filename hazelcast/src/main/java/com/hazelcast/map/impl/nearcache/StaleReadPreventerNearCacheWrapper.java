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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.monitor.NearCacheStats;

/**
 * Guards a {@link NearCache} against stale reads by using {@link KeyStateMarker}
 *
 * @see KeyStateMarker
 */
public final class StaleReadPreventerNearCacheWrapper<K, V> implements NearCache<K, V> {

    private final NearCache<K, V> nearCache;
    private final KeyStateMarker keyStateMarker;

    private StaleReadPreventerNearCacheWrapper(NearCache<K, V> nearCache, int markerCount) {
        this.nearCache = nearCache;
        this.keyStateMarker = new KeyStateMarkerImpl(markerCount);
    }

    public static <KEY, VALUE> NearCache<KEY, VALUE> wrapAsStaleReadPreventerNearCache(NearCache<KEY, VALUE> nearCache,
                                                                                       int markerCount) {
        return new StaleReadPreventerNearCacheWrapper<KEY, VALUE>(nearCache, markerCount);
    }

    @Override
    public String getName() {
        return nearCache.getName();
    }

    @Override
    public V get(K key) {
        return nearCache.get(key);
    }

    @Override
    public void put(K key, V value) {
        nearCache.put(key, value);
    }

    @Override
    public boolean remove(K key) {
        keyStateMarker.tryRemove(key);
        return nearCache.remove(key);
    }

    @Override
    public boolean isInvalidatedOnChange() {
        return nearCache.isInvalidatedOnChange();
    }

    @Override
    public void clear() {
        keyStateMarker.init();
        nearCache.clear();
    }

    @Override
    public void destroy() {
        keyStateMarker.init();
        nearCache.destroy();
    }

    @Override
    public InMemoryFormat getInMemoryFormat() {
        return nearCache.getInMemoryFormat();
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCache.getNearCacheStats();
    }

    @Override
    public Object selectToSave(Object... candidates) {
        return nearCache.selectToSave(candidates);
    }

    @Override
    public int size() {
        return nearCache.size();
    }

    public KeyStateMarker getKeyStateMarker() {
        return keyStateMarker;
    }
}

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
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static com.hazelcast.map.impl.nearcache.KeyStateMarker.STATE.MARKED;
import static com.hazelcast.map.impl.nearcache.KeyStateMarker.STATE.REMOVED;
import static com.hazelcast.map.impl.nearcache.KeyStateMarker.STATE.UNMARKED;
import static com.hazelcast.util.HashUtil.hashToIndex;

/**
 * Guards a {@link NearCache} against stale reads by using {@link KeyStateMarker}
 *
 * @see KeyStateMarker
 */
public final class StaleReadPreventerNearCacheWrapper implements NearCache, KeyStateMarker {

    private final NearCache nearCache;
    private final int markCount;

    private volatile AtomicIntegerArray marks;

    private StaleReadPreventerNearCacheWrapper(NearCache nearCache, int markCount) {
        this.nearCache = nearCache;
        this.markCount = markCount;
        this.marks = new AtomicIntegerArray(markCount);
    }

    public static NearCache wrapAsStaleReadPreventerNearCache(NearCache nearCache, int markerCount) {
        return new StaleReadPreventerNearCacheWrapper(nearCache, markerCount);
    }

    @Override
    public String getName() {
        return nearCache.getName();
    }

    @Override
    public Object get(Object key) {
        return nearCache.get(key);
    }

    @Override
    public void put(Object key, Object value) {
        nearCache.put(key, value);
    }

    @Override
    public boolean remove(Object key) {
        tryRemove(key);
        return nearCache.remove(key);
    }

    @Override
    public boolean isInvalidateOnChange() {
        return nearCache.isInvalidateOnChange();
    }

    @Override
    public void clear() {
        init();
        nearCache.clear();
    }

    @Override
    public void destroy() {
        init();
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

    @Override
    public boolean tryMark(Object key) {
        return casState(key, UNMARKED, MARKED);
    }

    @Override
    public boolean tryUnmark(Object key) {
        return casState(key, MARKED, UNMARKED);
    }

    @Override
    public boolean tryRemove(Object key) {
        return casState(key, MARKED, REMOVED);
    }

    @Override
    public void forceUnmark(Object key) {
        int slot = getSlot(key);
        marks.set(slot, UNMARKED.getState());
    }

    @Override
    public void init() {
        marks = new AtomicIntegerArray(markCount);
    }

    private boolean casState(Object key, STATE expect, STATE update) {
        int slot = getSlot(key);
        return marks.compareAndSet(slot, expect.getState(), update.getState());
    }

    private int getSlot(Object key) {
        int hash = key instanceof Data ? ((Data) key).getPartitionHash() : key.hashCode();
        return hashToIndex(hash, markCount);
    }

    public KeyStateMarker getKeyStateMarker() {
        return this;
    }
}

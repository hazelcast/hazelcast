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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.SampleableConcurrentHashMap;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CacheRecordHashMap
        extends SampleableConcurrentHashMap<Data, CacheRecord>
        implements SampleableCacheRecordMap<Data, CacheRecord> {

    private static final long serialVersionUID = 1L;

    private final transient CacheContext cacheContext;

    public CacheRecordHashMap(int initialCapacity, CacheContext cacheContext) {
        super(initialCapacity);
        this.cacheContext = cacheContext;
    }

    public CacheRecordHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
            ConcurrentReferenceHashMap.ReferenceType keyType,
            ConcurrentReferenceHashMap.ReferenceType valueType,
            EnumSet<Option> options, CacheContext cacheContext) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
        this.cacheContext = cacheContext;
    }

    @Override
    public CacheRecord put(Data key, CacheRecord value) {
        CacheRecord oldRecord = super.put(key, value);
        if (oldRecord == null) {
            // New put
            cacheContext.increaseEntryCount();
        }
        return oldRecord;
    }

    @Override
    public CacheRecord putIfAbsent(Data key, CacheRecord value) {
        CacheRecord oldRecord = super.putIfAbsent(key, value);
        if (oldRecord == null) {
            // New put
            cacheContext.increaseEntryCount();
        }
        return oldRecord;
    }

    @Override
    public CacheRecord remove(Object key) {
        CacheRecord removedRecord = super.remove(key);
        if (removedRecord != null) {
            // Removed
            cacheContext.decreaseEntryCount();
        }
        return removedRecord;
    }

    @Override
    public boolean remove(Object key, Object value) {
        boolean removed = super.remove(key, value);
        if (removed) {
            // Removed
            cacheContext.decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public void clear() {
        final int sizeBeforeClear = size();
        super.clear();
        cacheContext.decreaseEntryCount(sizeBeforeClear);
    }

    public class EvictableSamplingEntry extends SamplingEntry implements EvictionCandidate {

        public EvictableSamplingEntry(Data key, CacheRecord value) {
            super(key, value);
        }

        @Override
        public Object getAccessor() {
            return getKey();
        }

        @Override
        public Evictable getEvictable() {
            return getValue();
        }

    }

    @Override
    protected EvictableSamplingEntry createSamplingEntry(Data key, CacheRecord value) {
        return new EvictableSamplingEntry(key, value);
    }

    @Override
    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        List<Data> keys = new ArrayList<Data>();
        int tableIndex = fetch(nextTableIndex, size, keys);
        return new CacheKeyIteratorResult(keys, tableIndex);
    }

    @Override
    public <C extends EvictionCandidate<Data, CacheRecord>> int evict(Iterable<C> evictionCandidates,
            EvictionListener<Data, CacheRecord> evictionListener) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, CacheRecord> evictionCandidate : evictionCandidates) {
            if (remove(evictionCandidate.getAccessor()) != null) {
                actualEvictedCount++;
                if (evictionListener != null) {
                    evictionListener.onEvict(evictionCandidate.getAccessor(), evictionCandidate.getEvictable());
                }
            }
        }
        return actualEvictedCount;
    }

    @Override
    public Iterable<EvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }
}

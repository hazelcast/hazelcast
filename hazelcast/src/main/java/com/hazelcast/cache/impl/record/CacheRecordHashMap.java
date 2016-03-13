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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.SampleableConcurrentHashMap;

import java.util.ArrayList;
import java.util.List;

public class CacheRecordHashMap
        extends SampleableConcurrentHashMap<Data, CacheRecord>
        implements SampleableCacheRecordMap<Data, CacheRecord> {

    private static final long serialVersionUID = 1L;

    private final transient CacheContext cacheContext;
    private boolean entryCountingEnable;

    public CacheRecordHashMap(int initialCapacity, CacheContext cacheContext) {
        super(initialCapacity);
        this.cacheContext = cacheContext;
    }

    // Called by only same partition thread. So there is no synchronization and visibility problem.
    @Override
    public void setEntryCounting(boolean enable) {
        if (enable) {
            if (!entryCountingEnable) {
                // It was disable before but now it will be enable.
                // Therefore, we increase the entry count as size of records.
                cacheContext.increaseEntryCount(size());
            }
        } else {
            if (entryCountingEnable) {
                // It was enable before but now it will be disable.
                // Therefore, we decrease the entry count as size of records.
                cacheContext.decreaseEntryCount(size());
            }
        }
        this.entryCountingEnable = enable;
    }

    @Override
    public CacheRecord put(Data key, CacheRecord value) {
        CacheRecord oldRecord = super.put(key, value);
        if (oldRecord == null && entryCountingEnable) {
            // New put
            cacheContext.increaseEntryCount();
        }
        return oldRecord;
    }

    @Override
    public CacheRecord putIfAbsent(Data key, CacheRecord value) {
        CacheRecord oldRecord = super.putIfAbsent(key, value);
        if (oldRecord == null && entryCountingEnable) {
            // New put
            cacheContext.increaseEntryCount();
        }
        return oldRecord;
    }

    @Override
    public CacheRecord remove(Object key) {
        CacheRecord removedRecord = super.remove(key);
        if (removedRecord != null && entryCountingEnable) {
            // Removed
            cacheContext.decreaseEntryCount();
        }
        return removedRecord;
    }

    @Override
    public boolean remove(Object key, Object value) {
        boolean removed = super.remove(key, value);
        if (removed && entryCountingEnable) {
            // Removed
            cacheContext.decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public void clear() {
        final int sizeBeforeClear = size();
        super.clear();
        if (entryCountingEnable) {
            cacheContext.decreaseEntryCount(sizeBeforeClear);
        }
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
            return (Evictable) getValue();
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

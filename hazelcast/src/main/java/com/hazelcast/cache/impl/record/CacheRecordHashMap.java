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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheEntriesWithCursor;
import com.hazelcast.cache.impl.CacheKeysWithCursor;
import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;

import javax.cache.expiry.ExpiryPolicy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SerializableByConvention
public class CacheRecordHashMap
        extends SampleableConcurrentHashMap<Data, CacheRecord>
        implements SampleableCacheRecordMap<Data, CacheRecord> {

    private static final long serialVersionUID = 1L;

    private final transient SerializationService serializationService;
    private final transient CacheContext cacheContext;
    private boolean entryCountingEnable;

    public CacheRecordHashMap(SerializationService serializationService,
                              int initialCapacity, CacheContext cacheContext) {
        super(initialCapacity);
        this.serializationService = serializationService;
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

    private class CacheEvictableSamplingEntry
            extends SamplingEntry<Data, CacheRecord>
            implements EvictionCandidate, CacheEntryView {

        CacheEvictableSamplingEntry(Data key, CacheRecord value) {
            super(key, value);
        }

        @Override
        public Object getAccessor() {
            return key;
        }

        @Override
        public Evictable getEvictable() {
            return value;
        }

        @Override
        public Object getKey() {
            return serializationService.toObject(key);
        }

        @Override
        public Object getValue() {
            return serializationService.toObject(value.getValue());
        }

        @Override
        public ExpiryPolicy getExpiryPolicy() {
            return serializationService.toObject(value.getExpiryPolicy());
        }

        @Override
        public long getCreationTime() {
            return value.getCreationTime();
        }

        @Override
        public long getExpirationTime() {
            return value.getExpirationTime();
        }

        @Override
        public long getLastAccessTime() {
            return value.getLastAccessTime();
        }

        @Override
        public long getHits() {
            return value.getHits();
        }
    }

    @Override
    protected CacheEvictableSamplingEntry createSamplingEntry(Data key, CacheRecord value) {
        return new CacheEvictableSamplingEntry(key, value);
    }

    @Override
    public CacheKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        List<Data> keys = new ArrayList<>(size);
        IterationPointer[] newIterationPointers = fetchKeys(pointers, size, keys);
        return new CacheKeysWithCursor(keys, newIterationPointers);
    }

    @Override
    public CacheEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        List<Map.Entry<Data, CacheRecord>> entries = new ArrayList<>(size);
        IterationPointer[] newIterationPointers = fetchEntries(pointers, size, entries);
        List<Map.Entry<Data, Data>> entriesData = new ArrayList<>(entries.size());
        for (Map.Entry<Data, CacheRecord> entry : entries) {
            CacheRecord record = entry.getValue();
            Data dataValue = serializationService.toData(record.getValue());
            entriesData.add(new AbstractMap.SimpleEntry<>(entry.getKey(), dataValue));
        }
        return new CacheEntriesWithCursor(entriesData, newIterationPointers);
    }

    @Override
    public <C extends EvictionCandidate<Data, CacheRecord>>
    boolean tryEvict(C evictionCandidate,
                     EvictionListener<Data, CacheRecord> evictionListener) {
        if (evictionCandidate == null) {
            return false;
        }
        if (remove(evictionCandidate.getAccessor()) == null) {
            return false;
        }
        if (evictionListener != null) {
            evictionListener.onEvict(evictionCandidate.getAccessor(), evictionCandidate.getEvictable(), false);
        }
        return true;
    }

    @Override
    public Iterable<CacheEvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }
}

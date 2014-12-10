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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.EvictableSampleableConcurrentHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CacheRecordHashMap
        extends EvictableSampleableConcurrentHashMap<Data, CacheRecord>
        implements SampleableCacheRecordMap<Data, CacheRecord> {

    private final int globalEntryCapacity;

    // This can only be used from the single threaded partition thread system!
    // Never ever expose this number to a user thread!
    private int partitionEntryCount;

    public CacheRecordHashMap(int initialCapacity, int globalEntryCapacity) {
        super(initialCapacity);
        this.globalEntryCapacity = globalEntryCapacity;
    }

    @Override
    public CacheKeyIteratorResult fetchNext(int nextTableIndex, int size) {
        List<Data> keys = new ArrayList<Data>();
        int tableIndex = fetch(nextTableIndex, size, keys);
        return new CacheKeyIteratorResult(keys, tableIndex);
    }

    @Override
    public int evict(Iterable<EvictionCandidate<Data, CacheRecord>> evictionCandidates) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<Data, CacheRecord> evictionCandidate : evictionCandidates) {
            if (remove(evictionCandidate.getAccessor()) != null) {
                actualEvictedCount++;
            }
        }
        return actualEvictedCount;
    }

    @Override
    public CacheRecord put(Data key, CacheRecord value) {
        CacheRecord cacheRecord = super.put(key, value);
        if (cacheRecord == null) {
            partitionEntryCount++;
        }
        return cacheRecord;
    }

    @Override
    public CacheRecord putIfAbsent(Data key, CacheRecord value) {
        CacheRecord cacheRecord = super.putIfAbsent(key, value);
        if (cacheRecord == null) {
            partitionEntryCount++;
        }
        return cacheRecord;
    }

    @Override
    public CacheRecord remove(Object key) {
        CacheRecord cacheRecord = super.remove(key);
        if (cacheRecord != null) {
            partitionEntryCount--;
        }
        return cacheRecord;
    }

    @Override
    public boolean remove(Object key, Object value) {
        boolean result = super.remove(key, value);
        if (result) {
            partitionEntryCount--;
        }
        return result;
    }

    @Override
    public void putAll(Map<? extends Data, ? extends CacheRecord> m) {
        super.putAll(m);
        partitionEntryCount = size();
    }

    @Override
    public void clear() {
        super.clear();
        partitionEntryCount = 0;
    }

    @Override
    public long getPartitionEntryCount() {
        return partitionEntryCount;
    }

    @Override
    public long getGlobalEntryCapacity() {
        return globalEntryCapacity;
    }

    @Override
    public long getPartitionMemoryConsumption() {
        return -1;
    }

    @Override
    public Iterable<EvictionCandidate<Data, CacheRecord>> sample(int sampleCount) {
        return (Iterable) super.samplingIterator(sampleCount);
    }

    @Override
    public void cleanupSampling(Iterable<EvictionCandidate<Data, CacheRecord>> evictionCandidates) {
    }

    @Override
    public int fetch(int tableIndex, int size, List<Data> keys) {
        return super.fetch(tableIndex, size, keys);
    }
}

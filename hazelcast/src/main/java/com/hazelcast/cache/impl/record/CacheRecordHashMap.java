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
import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.SampleableConcurrentHashMap;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class CacheRecordHashMap
        extends SampleableConcurrentHashMap<Data, CacheRecord>
        implements SampleableCacheRecordMap<Data, CacheRecord> {

    public CacheRecordHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public CacheRecordHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
            ConcurrentReferenceHashMap.ReferenceType keyType,
            ConcurrentReferenceHashMap.ReferenceType valueType,
            EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
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
    public <C extends EvictionCandidate<Data, CacheRecord>> int evict(Iterable<C> evictionCandidates) {
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
    public Iterable<EvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }

}

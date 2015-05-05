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

package com.hazelcast.cache.impl.nearcache.impl.store;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;
import com.hazelcast.cache.impl.eviction.EvictionListener;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.cache.impl.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.util.SampleableConcurrentHashMap;

public class HeapNearCacheRecordMap<K, V extends NearCacheRecord>
        extends SampleableConcurrentHashMap<K, V>
        implements SampleableNearCacheRecordMap<K, V> {

    public HeapNearCacheRecordMap(int initialCapacity) {
        super(initialCapacity);
    }

    public class EvictableSamplingEntry extends SamplingEntry implements EvictionCandidate {

        public EvictableSamplingEntry(K key, V value) {
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
    protected <E extends SamplingEntry> E createSamplingEntry(K key, V value) {
        return (E) new EvictableSamplingEntry(key, value);
    }

    @Override
    public <C extends EvictionCandidate<K, V>> int evict(Iterable<C> evictionCandidates,
                                                         EvictionListener<K, V> evictionListener) {
        if (evictionCandidates == null) {
            return 0;
        }
        int actualEvictedCount = 0;
        for (EvictionCandidate<K, V> evictionCandidate : evictionCandidates) {
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

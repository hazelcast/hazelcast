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

package com.hazelcast.internal.nearcache.impl.store;

import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.impl.SampleableNearCacheRecordMap;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;

/**
 * {@link SampleableNearCacheRecordMap} implementation for on-heap Near Caches.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
@SerializableByConvention
public class HeapNearCacheRecordMap<K, V extends NearCacheRecord>
        extends SampleableConcurrentHashMap<K, V>
        implements SampleableNearCacheRecordMap<K, V> {

    private final SerializationService serializationService;

    HeapNearCacheRecordMap(SerializationService serializationService, int initialCapacity) {
        super(initialCapacity);
        this.serializationService = serializationService;
    }

    public class NearCacheEvictableSamplingEntry extends SamplingEntry<K, V> implements EvictionCandidate<K, V> {

        NearCacheEvictableSamplingEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public K getAccessor() {
            return key;
        }

        @Override
        public V getEvictable() {
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
        public long getCreationTime() {
            return value.getCreationTime();
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
    @SuppressWarnings("unchecked")
    protected <E extends SamplingEntry> E createSamplingEntry(K key, V value) {
        return (E) new NearCacheEvictableSamplingEntry(key, value);
    }

    @Override
    public <C extends EvictionCandidate<K, V>> boolean tryEvict(C evictionCandidate,
                                                                EvictionListener<K, V> evictionListener) {
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
    public Iterable<NearCacheEvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }
}

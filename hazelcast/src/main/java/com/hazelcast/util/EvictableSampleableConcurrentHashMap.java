/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.cache.impl.eviction.EvictableStore;
import com.hazelcast.cache.impl.eviction.EvictionCandidate;

import java.lang.ref.ReferenceQueue;
import java.util.EnumSet;

/**
 * An {@link com.hazelcast.cache.impl.eviction.EvictableStore} implementation build as a
 * {@link com.hazelcast.util.SampleableConcurrentHashMap} subclass. This implementation can
 * be used as a base class for storage implementation to use the eviction system.
 *
 * @param <K> key type
 * @param <V> value type which extends {@link com.hazelcast.cache.impl.eviction.Evictable}
 */
public class EvictableSampleableConcurrentHashMap<K, V extends Evictable>
        extends SampleableConcurrentHashMap<K, V>
        implements EvictableStore<K, V> {

    public EvictableSampleableConcurrentHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public EvictableSampleableConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
                                                ReferenceType keyType, ReferenceType valueType, EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

    @Override
    public int evict(Iterable<EvictionCandidate<K, V>> evictionCandidates) {
        return 0;
    }

    @Override
    public long getPartitionEntryCount() {
        return 0;
    }

    @Override
    public long getGlobalEntryCapacity() {
        return 0;
    }

    @Override
    public long getPartitionMemoryConsumption() {
        return 0;
    }

    @Override
    protected Segment<K, V> newSegment(int initialCapacity, float lf, ReferenceType keyType, ReferenceType valueType,
                                       boolean identityComparisons) {
        return new EvictableSegment<K, V>(initialCapacity, lf, keyType, valueType, identityComparisons);
    }

    static class EvictableSegment<K, V extends Evictable>
            extends Segment<K, V> {

        EvictableSegment(int initialCapacity, float lf, ReferenceType keyType, ReferenceType valueType,
                         boolean identityComparisons) {
            super(initialCapacity, lf, keyType, valueType, identityComparisons);
        }

        @Override
        HashEntry<K, V> newHashEntry(K key, int hash, HashEntry<K, V> next, V value) {
            return new EvictableHashEntry<K, V>(key, hash, next, value, keyType, valueType, refQueue);
        }
    }

    static class EvictableHashEntry<K, V extends Evictable>
            extends IterableSamplingEntry<K, V>
            implements EvictionCandidate<K, V> {

        EvictableHashEntry(K key, int hash, HashEntry<K, V> next, V value, ReferenceType keyType, ReferenceType valueType,
                           ReferenceQueue<Object> refQueue) {
            super(key, hash, next, value, keyType, valueType, refQueue);
        }

        @Override
        public K getAccessor() {
            return key();
        }

        @Override
        public V getEvictable() {
            return value();
        }
    }
}

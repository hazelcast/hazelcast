/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.EvictionCandidate;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.SampleableConcurrentHashMap;
import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;

/**
 * Evictable concurrent hash map implementation.
 *
 * @see SampleableConcurrentHashMap
 * @see SampleableEvictableStore
 */
@SerializableByConvention
public class QueryCacheRecordHashMap extends SampleableConcurrentHashMap<Data, QueryCacheRecord>
        implements SampleableEvictableStore<Data, QueryCacheRecord> {

    private final SerializationService serializationService;

    public QueryCacheRecordHashMap(SerializationService serializationService, int initialCapacity) {
        super(initialCapacity);
        this.serializationService = serializationService;
    }

    /**
     * @see com.hazelcast.internal.util.SampleableConcurrentHashMap.SamplingEntry
     * @see EvictionCandidate
     */
    class QueryCacheEvictableSamplingEntry
            extends SamplingEntry<Data, QueryCacheRecord>
            implements EvictionCandidate {

        QueryCacheEvictableSamplingEntry(Data key, QueryCacheRecord value) {
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
    protected QueryCacheEvictableSamplingEntry createSamplingEntry(Data key, QueryCacheRecord value) {
        return new QueryCacheEvictableSamplingEntry(key, value);
    }

    @Override
    public <C extends EvictionCandidate<Data, QueryCacheRecord>>
    boolean tryEvict(C evictionCandidate, EvictionListener<Data, QueryCacheRecord> evictionListener) {
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
    public Iterable<QueryCacheEvictableSamplingEntry> sample(int sampleCount) {
        return super.getRandomSamples(sampleCount);
    }
}

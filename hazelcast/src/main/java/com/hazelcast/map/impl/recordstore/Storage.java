/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;

/**
 * Represents actual storage layer behind a {@link RecordStore}.
 * Includes basic storage operations.
 *
 * @param <K> the key type to be put in this storage.
 * @param <R> the value type to be put in this storage.
 */
public interface Storage<K, R> {

    void put(K key, R record);

    void updateRecordValue(K key, R record, Object value);

    R get(K key);

    /**
     * Gives the same result as {@link #get(Object)}, but with the additional constraint
     * that the supplied key must not just be equal to, but be exactly the same key blob (at the
     * same memory address) as the one stored. The implementation of this method is only needed
     * for the HD memory-based implementations.
     */
    R getIfSameKey(K key);

    void removeRecord(R record);

    boolean containsKey(K key);

    Collection<R> values();

    int size();

    boolean isEmpty();

    void clear(boolean isDuringShutdown);

    void destroy(boolean isDuringShutdown);

    EntryCostEstimator getEntryCostEstimator();

    void setEntryCostEstimator(EntryCostEstimator entryCostEstimator);

    void disposeDeferredBlocks();

    /**
     * Used for sampling based eviction, returns sampled entries.
     *
     * @param sampleCount sample count.
     * @return sampled entries.
     */
    Iterable<LazyEntryViewFromRecord> getRandomSamples(int sampleCount);

    MapKeysWithCursor fetchKeys(int tableIndex, int size);

    MapEntriesWithCursor fetchEntries(int tableIndex, int size, SerializationService serializationService);

}

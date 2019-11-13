/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.Record;

import java.util.Collection;
import java.util.Iterator;

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

    /**
     * Returned iterator from this method doesn't throw {@link java.util.ConcurrentModificationException} to fail fast.
     * Because fail fast may not be the desired behaviour always. For example if you are caching an iterator as in
     * {@link AbstractEvictableRecordStore#expirationIterator} and you know that in next rounds you will
     * eventually visit all entries, you don't need fail fast behaviour.
     *
     * Note that returned iterator is not thread-safe !!!
     *
     * @return new iterator instance
     */
    Iterator<R> mutationTolerantIterator();

    int size();

    boolean isEmpty();

    /**
     * @param isDuringShutdown only used by hot-restart.
     */
    void clear(boolean isDuringShutdown);

    void destroy(boolean isDuringShutdown);

    EntryCostEstimator getEntryCostEstimator();

    void setEntryCostEstimator(EntryCostEstimator entryCostEstimator);

    default void disposeDeferredBlocks() {
        // NOP intentionally.
    }

    /**
     * Used for sampling based eviction, returns sampled entries.
     *
     * @param sampleCount sample count.
     * @return sampled entries.
     */
    Iterable<EntryView> getRandomSamples(int sampleCount);

    /**
     * Fetch minimally {@code size} keys from the {@code tableIndex} position. The key is fetched on-heap.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items. This can happen if we cannot easily resume
     * from the last returned item by receiving the {@code tableIndex} of the last item. The index can represent a bucket
     * with multiple items and in this case the returned object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param tableIndex the index (position) from which to resume
     * @param size       the minimal count of returned items
     * @return fetched keys and the table index for keys AFTER the last returned key
     */
    MapKeysWithCursor fetchKeys(int tableIndex, int size);

    /**
     * Fetch minimally {@code size} items from the {@code tableIndex} position. Both the key and value are fetched on-heap.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items. This can happen if we cannot easily resume
     * from the last returned item by receiving the {@code tableIndex} of the last item. The index can represent a bucket
     * with multiple items and in this case the returned object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param tableIndex the index (position) from which to resume
     * @param size       the minimal count of returned items
     * @return fetched entries and the table index for entries AFTER the last returned entry
     */
    MapEntriesWithCursor fetchEntries(int tableIndex, int size, SerializationService serializationService);

    Record extractRecordFrom(EntryView evictableEntryView);
}

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

import com.hazelcast.cache.impl.CacheEntriesWithCursor;
import com.hazelcast.cache.impl.CacheKeysWithCursor;
import com.hazelcast.internal.eviction.EvictableStore;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;

import java.util.Map;

/**
 * Contract point for storing {@link CacheRecord}s.
 *
 * @param <K> type of the key of {@link CacheRecord} to be stored
 * @param <V> type of the value of {@link CacheRecord} to be stored
 */
public interface CacheRecordMap<K extends Data, V extends CacheRecord>
        extends Map<K, V>, EvictableStore<K, V> {

    /**
     * Sets the entry counting behaviour.
     * Because, records on backup partitions should not be counted.
     *
     * @param enable enables the entry counting behaviour if it is <tt>true</tt>, otherwise disables.
     */
    void setEntryCounting(boolean enable);

    /**
     * Fetch minimally {@code size} keys from the {@code pointers} position.
     * The key is fetched on-heap.
     * The method may return less keys if iteration has completed.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items.
     * This can happen if we cannot easily resume from the last returned item
     * by receiving the {@code tableIndex} of the last item. The index can
     * represent a bucket with multiple items and in this case the returned
     * object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param pointers the pointers defining the state of iteration
     * @param size     the minimal count of returned items, unless iteration has completed
     * @return fetched keys and the new iteration state
     */
    CacheKeysWithCursor fetchKeys(IterationPointer[] pointers, int size);

    /**
     * Fetch minimally {@code size} items from the {@code pointers} position.
     * Both the key and value are fetched on-heap.
     * <p>
     * NOTE: The implementation is free to return more than {@code size} items.
     * This can happen if we cannot easily resume from the last returned item
     * by receiving the {@code tableIndex} of the last item. The index can
     * represent a bucket with multiple items and in this case the returned
     * object will contain all items in that bucket, regardless if we exceed
     * the requested {@code size}.
     *
     * @param pointers the pointers defining the state of iteration
     * @param size     the minimal count of returned items
     * @return fetched entries and the new iteration state
     */
    CacheEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size);

}

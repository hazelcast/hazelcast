/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.eviction.EvictableStore;
import com.hazelcast.nio.serialization.Data;

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
     * Fetches keys in bulk as specified <tt>size</tt> at most.
     *
     * @param nextTableIndex starting point for fetching
     * @param size maximum bulk size to fetch the keys
     * @return the {@link CacheKeyIteratorResult} instance contains fetched keys
     */
    CacheKeyIteratorResult fetchNext(int nextTableIndex, int size);

}

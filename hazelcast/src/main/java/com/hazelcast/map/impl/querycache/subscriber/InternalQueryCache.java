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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Internal interface which adds some internally
 * used methods to {@code QueryCache} interface.
 *
 * @param <K> the key type for this {@code QueryCache}
 * @param <V> the value type for this {@code QueryCache}
 */
public interface InternalQueryCache<K, V> extends QueryCache<K, V> {

    void set(K key, V value, EntryEventType eventType);

    /**
     * Populate query cache with initial set of entries as {@link Data}.
     * Triggers an {@link EntryEventType#ADDED ADDED} event for each entry that
     * is added to the query cache. Note that not all events may be added
     * to the query cache if the query cache is configured with an eviction
     * policy with a maximum {@code ENTRY_COUNT}.
     *
     * @param entries key-value pairs iterator.
     */
    void prepopulate(Iterator<Map.Entry<Data, Data>> entries);

    void delete(Object key, EntryEventType eventType);

    /**
     * Scans all entries in this {@link QueryCache} to remove
     * matching ones with supplied {@code partitionId}
     *
     * @return number of entries removed
     */
    int removeEntriesOf(int partitionId);

    IMap<K, V> getDelegate();

    void clear();

    UUID getPublisherListenerId();

    void setPublisherListenerId(UUID publisherListenerId);

    /**
     * @return internally used ID for this query cache
     */
    String getCacheId();

    /**
     * Used to quit pre-population when max size is reached.
     *
     * @return {@code true} if this query cache is at
     * its max capacity, otherwise return {@code false}
     * @see QueryCacheConfig#isPopulate()
     */
    boolean reachedMaxCapacity();

    /**
     * @return extractors of this query cache instance.
     */
    Extractors getExtractors();

    /**
     * Recreates this query cache.
     *
     * Recreation steps are:
     * <ul>
     * <li>
     * Reset local subscribers' state, clear all cached entries
     * </li>
     * <li>
     * Recreate/reset publisher (server) side resources
     * by using this subscribers'metadata e.g. on server
     * restart we can recreate server side resources.
     * </li>
     * </ul>
     */
    void recreate();
}

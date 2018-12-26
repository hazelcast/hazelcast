/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Internal interface which adds some internally used methods
 * to {@code QueryCache} interface.
 *
 * @param <K> the key type for this {@code QueryCache}
 * @param <V> the value type for this {@code QueryCache}
 */
public interface InternalQueryCache<K, V> extends QueryCache<K, V> {

    void set(K key, V value, EntryEventType eventType);

    /**
     * Used during initial population of query cache. Initially fetched data
     * from cluster will be written to query cache by the help of this method.
     */
    void prepopulate(K key, V value);

    void delete(Object key, EntryEventType eventType);

    /**
     * Scans all entries in this {@link QueryCache} to remove matching ones
     * with supplied {@code partitionId}
     *
     * @return number of entries removed
     */
    int removeEntriesOf(int partitionId);

    IMap<K, V> getDelegate();

    Indexes getIndexes();

    void clear();

    void setPublisherListenerId(String publisherListenerId);

    /**
     * @return internally used ID for this query cache
     */
    String getCacheId();

    /**
     * Used to quit pre-population when max size is reached.
     *
     * @return {@code true} if this query cache is at its max capacity,
     * otherwise return {@code false}
     * @see com.hazelcast.config.QueryCacheConfig#populate
     */
    boolean reachedMaxCapacity();

    /**
     * @return extractors of this query cache instance.
     */
    Extractors getExtractors();
}

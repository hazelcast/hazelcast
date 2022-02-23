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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.Iterator;

/**
 * Internal API for {@link com.hazelcast.cache.ICache} implementations.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see com.hazelcast.cache.ICache
 * @since 3.5
 */
public interface ICacheInternal<K, V> extends ICache<K, V> {

    /**
     * Opens cache if available (not destroyed).
     * If cache is already open, does nothing.
     *
     * @throws IllegalStateException if cache is already destroyed
     */
    void open();

    /**
     * Registers the provided listener configuration.
     *
     * @param cacheEntryListenerConfiguration The cache configuration to be used for registering the entry listener
     * @param addToConfig                     If true, the configuration is added to the existing listeners in the cache config.
     */
    void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration, boolean addToConfig)
            throws IllegalArgumentException;

    /**
     * Iterator for the single specified partition {@link ICache}
     *
     * @param fetchSize      batch fetching size
     * @param partitionId    partition ID of the entries to iterate on
     * @param prefetchValues prefetch values
     * @return iterator for the entries of the partition
     */
    Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues);

    /**
     * Iterable for the single specified partition {@link ICache}
     *
     * @param fetchSize      batch fetching size
     * @param partitionId    partition ID of the entries to iterate on
     * @param prefetchValues prefetch values
     * @return iterator for the entries of the partition
     */
    Iterable<Entry<K, V>> iterable(int fetchSize, int partitionId, boolean prefetchValues);

    /**
     * Sets relevant {@link HazelcastCacheManager} to client/server.
     *
     * @param cacheManager client or server {@link HazelcastCacheManager}
     */
    void setCacheManager(HazelcastCacheManager cacheManager);

    /**
     * Reset cache manager of this cache proxy to {@code null}. Whenever a Cache is not managed any more
     * (for example after {@code Cache.close()} has been called), its {@code CacheManager} should be reset.
     */
    void resetCacheManager();
}

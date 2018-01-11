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

package com.hazelcast.cache;

import com.hazelcast.internal.eviction.EvictableEntryView;

/**
 * Entry info for cache record.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public interface CacheEntryView<K, V> extends EvictableEntryView<K, V> {

    /**
     * Gets the key of the cache entry.
     *
     * @return the key of the cache entry
     */
    K getKey();

    /**
     * Gets the value of the cache entry.
     *
     * @return the value of the cache entry
     */
    V getValue();

    /**
     * Gets the expiration time in milliseconds of the cache entry.
     *
     * @return the expiration time in milliseconds of the cache entry
     */
    long getExpirationTime();

    /**
     * Gets the last access time in milliseconds of the cache entry.
     *
     * @return the last access time in milliseconds of the cache entry
     */
    long getLastAccessTime();

    /**
     * Gets the count of how many time this cache entry has been accessed.
     *
     * @return the count of how many time this cache entry has been accessed
     */
    long getAccessHit();
}

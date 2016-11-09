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

package com.hazelcast.cache.impl;

import javax.cache.configuration.CacheEntryListenerConfiguration;

/**
 * Used internally to register the cache entry listener with adding the configuration to the cache config or not.
 * @param <K>
 * @param <V>
 */
public interface InternalCacheEntryListenerRegisterer<K, V> {
    /**
     *
     * @param cacheEntryListenerConfiguration The cache configuration to be used for registering the entry listener
     * @param addToConfig If true, the configuration is added to the cache config listeners.
     */
    void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration, boolean addToConfig)
            throws IllegalArgumentException;
}

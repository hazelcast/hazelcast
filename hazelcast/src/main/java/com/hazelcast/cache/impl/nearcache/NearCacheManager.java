/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache;

import com.hazelcast.config.NearCacheConfig;

import java.util.Collection;

/**
 * {@link NearCacheManager} is the contract point to manage all existing
 * {@link com.hazelcast.cache.impl.nearcache.NearCache} instances.
 */
public interface NearCacheManager {

    /**
     * Gets the {@link com.hazelcast.cache.impl.nearcache.NearCache} instance
     * associated with given <code>name</code>.
     *
     * @param name the name of the {@link com.hazelcast.cache.impl.nearcache.NearCache} instance will be got
     * @param <K> the type of the key for Near Cache
     * @param <V> the type of the value for Near Cache
     * @return the {@link com.hazelcast.cache.impl.nearcache.NearCache} instance
     *         associated with given <code>name</code>
     */
    <K, V> NearCache<K, V> getNearCache(String name);

    /**
     * Creates a new {@link com.hazelcast.cache.impl.nearcache.NearCache} with given configurations
     * or returns existing one.
     *
     * @param name              the name of the {@link com.hazelcast.cache.impl.nearcache.NearCache}
     *                          to be created or existing one
     * @param nearCacheConfig   the {@link com.hazelcast.config.NearCacheConfig} of the
     *                          {@link com.hazelcast.cache.impl.nearcache.NearCache} to be created
     * @param nearCacheContext  the {@link com.hazelcast.cache.impl.nearcache.NearCacheContext} of the
     *                          {@link com.hazelcast.cache.impl.nearcache.NearCache} to be created
     * @param <K> the key type of the {@link com.hazelcast.cache.impl.nearcache.NearCache}
     * @param <V> the value type of the {@link com.hazelcast.cache.impl.nearcache.NearCache}
     * @return the created or existing {@link com.hazelcast.cache.impl.nearcache.NearCache} instance
     *         associated with given <code>name</code>
     */
    <K, V> NearCache<K, V> createNearCacheIfAbsent(String name, NearCacheConfig nearCacheConfig,
                                                   NearCacheContext nearCacheContext);

    /**
     * Lists all existing {@link com.hazelcast.cache.impl.nearcache.NearCache} instances.
     *
     * @return all existing {@link com.hazelcast.cache.impl.nearcache.NearCache} instances
     */
    Collection<NearCache> listAllNearCaches();

    /**
     * Clears {@link com.hazelcast.cache.impl.nearcache.NearCache} instance associated with given <code>name</code>
     * but not removes it.
     *
     * @param name name of the {@link com.hazelcast.cache.impl.nearcache.NearCache} to be cleared
     * @return <code>true</code> if {@link com.hazelcast.cache.impl.nearcache.NearCache}
     *         was found and cleared, otherwise <code>false</code>
     */
    boolean clearNearCache(String name);

    /**
     * Clears all defined {@link com.hazelcast.cache.impl.nearcache.NearCache} instances.
     */
    void clearAllNearCaches();

    /**
     * Destroys {@link com.hazelcast.cache.impl.nearcache.NearCache} instance associated with given <code>name</code>
     * and also removes it.
     *
     * @param name name of the {@link com.hazelcast.cache.impl.nearcache.NearCache} to be destroyed
     * @return <code>true</code> if {@link com.hazelcast.cache.impl.nearcache.NearCache}
     *         was found and destroyed, otherwise <code>false</code>
     */
    boolean destroyNearCache(String name);

    /**
     * Destroys all defined {@link com.hazelcast.cache.impl.nearcache.NearCache} instances.
     */
    void destroyAllNearCaches();

}

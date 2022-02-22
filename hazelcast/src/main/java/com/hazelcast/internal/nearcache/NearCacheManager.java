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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;

import java.util.Collection;

/**
 * {@link NearCacheManager} is the contract point to manage all existing {@link NearCache} instances.
 */
public interface NearCacheManager {

    /**
     * Gets the {@link NearCache} instance associated with given {@code name}.
     *
     * @param name the name of the {@link NearCache} instance will be got
     * @param <K>  the type of the key for Near Cache
     * @param <V>  the type of the value for Near Cache
     * @return the {@link NearCache} instance
     * associated with given {@code name}
     */
    <K, V> NearCache<K, V> getNearCache(String name);

    /**
     * Creates a new {@link NearCache} with given configurations
     * or returns existing one.
     *
     * @param name            the name of the {@link NearCache} to be created or existing one
     * @param nearCacheConfig the {@link NearCacheConfig} of the {@link NearCache} to be created
     * @param <K>             the key type of the {@link NearCache}
     * @param <V>             the value type of the {@link NearCache}
     * @return the created or existing {@link NearCache} instance associated with given {@code name}
     */
    <K, V> NearCache<K, V> getOrCreateNearCache(String name, NearCacheConfig nearCacheConfig);

    /**
     * Triggers the pre-loading of the created {@link
     * NearCache} via the supplied {@link DataStructureAdapter}.
     *
     * @param nearCache {@link NearCache} instance
     * @param dataStructureAdapter the {@link
     *                             DataStructureAdapter} of the {@link NearCache} to be created
     */
    void startPreloading(NearCache nearCache, DataStructureAdapter dataStructureAdapter);

    /**
     * Lists all existing {@link NearCache} instances.
     *
     * @return all existing {@link NearCache} instances
     */
    Collection<NearCache> listAllNearCaches();

    /**
     * Clears {@link NearCache} instance associated with given {@code name} but not removes it.
     *
     * @param name name of the {@link NearCache} to be cleared
     * @return {@code true} if {@link NearCache} was found and cleared, {@code false} otherwise
     */
    boolean clearNearCache(String name);

    /**
     * Clears all defined {@link NearCache} instances.
     */
    void clearAllNearCaches();

    /**
     * Destroys {@link NearCache} instance associated with given {@code name} and also removes it.
     *
     * @param name name of the {@link NearCache} to be destroyed
     * @return {@code true} if {@link NearCache} was found and destroyed, {@code false} otherwise
     */
    boolean destroyNearCache(String name);

    /**
     * Destroys all defined {@link NearCache} instances.
     */
    void destroyAllNearCaches();
}

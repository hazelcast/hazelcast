/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ManagedService;

import java.util.Collection;
import java.util.List;

/**
 * Responsible for local and remote near-cache invalidation.
 * Local near-caches are node local, remote near-caches can be exist on remote nodes or clients.
 *
 * @since 3.6
 */
public interface NearCacheInvalidator {

    /**
     * Only invalidates local near-cache on a node.
     *
     * @param mapName name of the map.
     * @param key     key of the entry to be removed.
     */
    void invalidateLocalNearCache(String mapName, Data key);

    /**
     * Only invalidates local near-cache on a node.
     *
     * @param mapName name of the map.
     * @param keys    keys of the entries to be removed.
     */
    void invalidateLocalNearCache(String mapName, Collection<Data> keys);


    /**
     * Only clears local near-cache.
     *
     * @param mapName    name of the map.
     * @param sourceUuid caller uuid
     */
    void clearLocalNearCache(String mapName, String sourceUuid);

    /**
     * Clears local and remote near-caches.
     * Local near-caches are node local, remote near-caches can be exist on remote nodes or clients.
     *
     * @param mapName    name of the map.
     * @param owner      <code>true</code> if this method is called from partition owner, otherwise <code>false</code>.
     * @param sourceUuid caller uuid
     */
    void clearNearCaches(String mapName, boolean owner, String sourceUuid);

    /**
     * Invalidates local and remote near-caches.
     * Local near-caches are node local, remote near-caches can be exist on remote nodes or clients.
     *
     * @param mapName    name of the map.
     * @param key        key of the entry to be removed.
     * @param sourceUuid caller uuid
     */
    void invalidateNearCaches(String mapName, Data key, String sourceUuid);

    /**
     * Invalidates local and remote near-caches.
     * Local near-caches are node local, remote near-caches can be exist on remote nodes or clients.
     *
     * @param mapName    name of the map.
     * @param keys       keys of the entries to be removed.
     * @param sourceUuid caller uuid
     */
    void invalidateNearCaches(String mapName, List<Data> keys, String sourceUuid);

    /**
     * Removes supplied maps invalidation queue and flushes its content.
     * This method is called when removing a near-cache with
     * {@link com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)}
     *
     * @param mapName name of the map.
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    void flushAndRemoveInvalidationQueue(String mapName);

    /**
     * Resets this invalidator back to its initial state.
     * Aimed to call with {@link ManagedService#reset()}
     *
     * @see {@link ManagedService#reset()}
     */
    void reset();

    /**
     * Shuts down this invalidator and releases used resources.
     * Aimed to call with {@link com.hazelcast.spi.ManagedService#shutdown(boolean)}
     *
     * @see {@link com.hazelcast.spi.ManagedService#shutdown(boolean)}
     */
    void shutdown();
}

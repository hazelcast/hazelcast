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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ManagedService;

import java.util.List;

/**
 * Responsible for local and remote Near Cache invalidation.
 * Local Near Caches are node local, remote Near Caches can be exist on remote nodes or clients.
 *
 * @since 3.6
 */
public interface NearCacheInvalidator {

    /**
     * Invalidates local and remote Near Caches.
     * Local Near Caches are node local, remote Near Caches can be exist on remote members or clients.
     *
     * @param mapName    name of the map.
     * @param key        key of the entry to be removed from Near Cache.
     * @param sourceUuid caller uuid
     */
    void invalidate(String mapName, Data key, String sourceUuid);

    /**
     * Invalidates local and remote Near Caches.
     * Local Near Caches are node local, remote Near Caches can be exist on remote members or clients.
     *
     * @param mapName    name of the map.
     * @param keys       keys of the entries to be removed from Near Cache.
     * @param sourceUuid caller uuid
     */
    void invalidate(String mapName, List<Data> keys, String sourceUuid);

    /**
     * Clears local members Near Cache.
     *
     * @param mapName name of the map.
     */
    void clearLocalNearCache(String mapName);

    /**
     * Send clear event to client-side Near Cache invalidation listeners.
     *
     * @param mapName    name of the map.
     * @param sourceUuid caller uuid
     */
    void sendClientNearCacheClearEvent(String mapName, String sourceUuid);

    /**
     * Removes supplied maps invalidation queue and flushes its content.
     * This method is called when removing a Near Cache with
     * {@link com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)}
     *
     * @param mapName name of the map.
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    void destroy(String mapName);

    /**
     * Resets this invalidator back to its initial state.
     * Aimed to call with {@link ManagedService#reset()}
     *
     * @see ManagedService#reset()
     */
    void reset();

    /**
     * Shuts down this invalidator and releases used resources.
     * Aimed to call with {@link com.hazelcast.spi.ManagedService#shutdown(boolean)}
     *
     * @see ManagedService#shutdown(boolean)
     */
    void shutdown();
}

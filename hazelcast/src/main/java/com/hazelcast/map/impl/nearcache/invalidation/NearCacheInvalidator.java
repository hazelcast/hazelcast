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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ManagedService;

/**
 * Invalidates near-caches.
 *
 * @since 3.6
 */
public interface NearCacheInvalidator {

    /**
     * Invalidates supplied key from this maps near-caches.
     *
     * @param key        key of the entry to be removed from near-cache.
     * @param mapName    name of the map.
     * @param sourceUuid caller uuid
     */
    void invalidate(Data key, String mapName, String sourceUuid);

    /**
     * Removes supplied maps invalidation queue and flushes its content.
     * This method is called when removing a Near Cache with
     * {@link com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)}
     *
     * @param mapName    name of the map.
     * @param sourceUuid caller uuid
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    void destroy(String mapName, String sourceUuid);

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

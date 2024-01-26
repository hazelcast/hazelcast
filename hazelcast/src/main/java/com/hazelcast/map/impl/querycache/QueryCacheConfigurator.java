/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.QueryCacheConfig;

/**
 * {@link QueryCacheConfig} supplier abstraction.
 * Helps to provide different implementations on node or client subscriber sides.
 */
public interface QueryCacheConfigurator {

    /**
     * Returns {@link QueryCacheConfig} for the requested {@code cacheName}
     *
     * @param mapName   underlying IMap name for query cache.
     * @param cacheName query cache name.
     * @return {@link QueryCacheConfig} for the requested {@code cacheName}.
     */
    QueryCacheConfig getOrCreateConfiguration(String mapName, String cacheName, String cacheId);

    /**
     * Returns {@link QueryCacheConfig} for the requested {@code cacheName} or null
     *
     * @param mapName   underlying IMap name for query cache.
     * @param cacheName query cache name.
     * @return {@link QueryCacheConfig} for the requested {@code cacheName}.
     */
    QueryCacheConfig getOrNull(String mapName, String cacheName, String cacheId);

    /**
     * Removes corresponding configuration for the supplied {@code cacheName}
     *
     * @param mapName   underlying IMap name for query cache.
     * @param cacheName query cache name.
     */
    void removeConfiguration(String mapName, String cacheName);
}

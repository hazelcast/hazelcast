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

package com.hazelcast.core;

import java.util.Properties;

/**
 * Provides Lifecycle support for the implementing MapLoader class.
 * MapLoader classes implementing MapLoaderLifecycleSupport
 * interface will be called by Hazelcast on init and destroy so that
 * implementation can do necessary configuration when initializing and
 * clean up when destroying.
 */
public interface MapLoaderLifecycleSupport {
    /**
     * Initializes this MapLoader implementation. Hazelcast will call
     * this method when the map is first used on the
     * HazelcastInstance. Implementation can
     * initialize required resources for the implementing
     * mapLoader, such as reading a config file and/or creating a
     * database connection.
     *
     * @param hazelcastInstance HazelcastInstance of this mapLoader.
     * @param properties        Properties set for this mapStore. see MapStoreConfig
     * @param mapName           name of the map.
     */
    void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName);

    /**
     * Hazelcast will call this method before shutting down.
     * This method can be overridden to clean up the resources
     * held by this map loader implementation, such as closing the
     * database connections, etc.
     */
    void destroy();
}

/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import java.util.Properties;

public abstract class AbstractMapStore<K, V> implements MapStore<K, V> {

    /**
     * Initializes this MapStore implementation. Hazelcast will call
     * this method when the map is first used on the
     * HazelcastInstance. This method can be overridden to
     * initialize required resources for this
     * mapStore such as reading a config file and/or creating
     * database connection.
     * <p/>
     * Default implementation does nothing.
     *
     * @param hazelcastInstance HazelcastInstance of this mapStore.
     * @param properties        Properties set for this mapStore. see MapStoreConfig
     * @param mapName           name of the map.
     */
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    }

    /**
     * Hazelcast will call this method before shutting down.
     * This method can be overridden to cleanup the resources
     * held by this map store implementation, such as closing the
     * database connections etc.
     * <p/>
     * Default implementation does nothing.
     */
    public void destroy() {
    }
}

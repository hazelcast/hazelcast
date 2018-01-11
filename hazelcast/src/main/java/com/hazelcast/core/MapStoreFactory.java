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

package com.hazelcast.core;

import java.util.Properties;

/**
 * Factory for MapLoader or MapStore instances, specifiable in MapStoreConfig.
 */
public interface MapStoreFactory<K, V> {

    /**
     * Produces a MapLoader or a MapStore for the given map name and properties.
     * This method will be executed as part of a Hazelcast member's post-join operations,
     * therefore it needs to adhere to the rules for post join operations, as described in
     * {@link com.hazelcast.spi.PostJoinAwareService#getPostJoinOperation()}.
     *
     * @param mapName    name of the distributed map that the produced MapLoader or MapStore will serve
     * @param properties the properties of the MapStoreConfig for the produced MapLoader or MapStore
     */
    MapLoader<K, V> newMapStore(String mapName, Properties properties);
}


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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * A context which provides/initializes map store specific functionality.
 * <p/>
 * Specifically,
 * <p/>
 * <li>creates map store implementation from map store configuration.</li>
 * <li>creates map store manager according to write-behind or write-through store configuration</li>
 * <li>loads initial keys if a loader defined.</li>
 */
public interface MapStoreContext {

    void start();

    void stop();

    MapStoreManager getMapStoreManager();

    MapStoreWrapper getMapStoreWrapper();

    boolean isWriteBehindMapStoreEnabled();

    SerializationService getSerializationService();

    ILogger getLogger(Class clazz);

    String getMapName();

    MapServiceContext getMapServiceContext();

    MapStoreConfig getMapStoreConfig();

    Iterable<Object> loadAllKeys();

    /**
     * @return true if MapLoader or MapStore is defined
     */
    boolean isMapLoader();
}

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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.AbstractMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class MapStoreWrapper extends AbstractMapStore implements MapLoader {

    private final MapLoader mapLoader;
    private final MapStore mapStore;
    private final Object initLock = new Object();
    private final boolean shouldInitialize;
    private final Object impl;
    private final HazelcastInstance hazelcastInstance;
    private final Properties properties;
    private final String mapName;

    volatile boolean initialized = false;

    public MapStoreWrapper(Object impl, HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.impl = impl;
        this.hazelcastInstance = hazelcastInstance;
        this.properties = properties;
        this.mapName = mapName;
        MapLoader loader = null;
        MapStore store = null;
        if (impl instanceof MapStore) {
            store = (MapStore) impl;
        }
        if (impl instanceof MapLoader) {
            loader = (MapLoader) impl;
        }
        this.mapLoader = loader;
        this.mapStore = store;
        this.shouldInitialize = (impl instanceof AbstractMapStore);
    }

    void checkInit() {
        if (!initialized && shouldInitialize) {
            synchronized (initLock) {
                if (!initialized) {
                    ((AbstractMapStore) impl).init(hazelcastInstance, properties, mapName);
                    initialized = true;
                }
            }
        }
    }

    public boolean isMapStore() {
        return (mapStore != null);
    }

    public boolean isMapLoader() {
        return (mapLoader != null);
    }

    public void delete(Object key) {
        checkInit();
        mapStore.delete(key);
    }

    public void store(Object key, Object value) {
        checkInit();
        mapStore.store(key, value);
    }

    public void storeAll(Map map) {
        checkInit();
        mapStore.storeAll(map);
    }

    public void deleteAll(Collection keys) {
        checkInit();
        mapStore.deleteAll(keys);
    }

    public Object load(Object key) {
        checkInit();
        return mapLoader.load(key);
    }

    public Map loadAll(Collection keys) {
        checkInit();
        return mapLoader.loadAll(keys);
    }
}

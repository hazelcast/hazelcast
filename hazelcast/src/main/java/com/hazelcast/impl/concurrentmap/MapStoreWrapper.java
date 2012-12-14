/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class MapStoreWrapper implements MapStore {

    private final MapLoader mapLoader;
    private final MapStore mapStore;
    private final Object initLock = new Object();
    private final boolean shouldInitialize;
    private final Object impl;
    private final HazelcastInstance hazelcastInstance;
    private final Properties properties;
    private final String mapName;

    private volatile boolean initialized = false;

    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public MapStoreWrapper(Object impl, HazelcastInstance hazelcastInstance,
                           Properties properties, String mapName, boolean enabled) {
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
        this.shouldInitialize = (impl instanceof MapLoaderLifecycleSupport);
        this.enabled.set(enabled);
    }

    void checkInit() {
        if (shouldInitialize && !initialized) {
            synchronized (initLock) {
                if (!initialized) {
                    ((MapLoaderLifecycleSupport) impl).init(hazelcastInstance,
                            properties, mapName);
                    initialized = true;
                }
            }
        }
    }

    public void setEnabled(boolean enable) {
        enabled.set(enable);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void destroy() {
        if (shouldInitialize && initialized) {
            synchronized (initLock) {
                if (initialized) {
                    ((MapLoaderLifecycleSupport) impl).destroy();
                    initialized = false;
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
        if (enabled.get()) {
            checkInit();
            mapStore.delete(key);
        }
    }

    public void store(Object key, Object value) {
        if (enabled.get()) {
            checkInit();
            mapStore.store(key, value);
        }
    }

    public void storeAll(Map map) {
        if (enabled.get()) {
            checkInit();
            mapStore.storeAll(map);
        }
    }

    public void deleteAll(Collection keys) {
        if (enabled.get()) {
            checkInit();
            mapStore.deleteAll(keys);
        }
    }

    public Set loadAllKeys() {
        if (enabled.get()) {
            checkInit();
            return mapLoader.loadAllKeys();
        }
        return null;
    }

    public Object load(Object key) {
        if (enabled.get()) {
            checkInit();
            return mapLoader.load(key);
        }
        return null;
    }

    public Map loadAll(Collection keys) {
        if (enabled.get()) {
            checkInit();
            return mapLoader.loadAll(keys);
        }
        return null;
    }

    @Override
    public String toString() {
        return "MapStoreWrapper{" + "mapName='" + mapName + '\''
                + ", mapStore=" + mapStore + ", mapLoader=" + mapLoader + '}';
    }
}

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.PostProcessingMapStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("unchecked")
public class MapStoreWrapper implements MapStore, MapLoaderLifecycleSupport {
    /**
     * An instance of {@link MapLoader} configured for this map
     * or {@code null} if none was provided.
     */
    private MapLoader mapLoader;
    /**
     * An instance of {@link MapStore} configured for this map
     * or {@code null} if none was provided.
     */
    private MapStore mapStore;

    private boolean withExpirationTime;

    private final String mapName;

    private final Object impl;

    public MapStoreWrapper(String mapName, Object impl) {
        this.mapName = mapName;
        this.impl = impl;
        MapLoader loader = null;
        MapStore store = null;
        if (impl instanceof MapStore) {
            store = (MapStore) impl;

        }
        if (impl instanceof MapLoader) {
            loader = (MapLoader) impl;
        }
        if (impl instanceof EntryLoader) {
            withExpirationTime = true;
        }
        this.mapLoader = loader;
        this.mapStore = store;
    }

    public MapStore getMapStore() {
        return mapStore;
    }

    @Override
    public void destroy() {
        if (impl instanceof MapLoaderLifecycleSupport) {
            ((MapLoaderLifecycleSupport) impl).destroy();
        }
    }

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        if (impl instanceof MapLoaderLifecycleSupport) {
            ((MapLoaderLifecycleSupport) impl).init(hazelcastInstance, properties, mapName);
        }
    }

    private boolean isMapStore() {
        return (mapStore != null);
    }

    /**
     * @return {@code true} if a {@link MapLoader} is configured for this map
     */
    public boolean isMapLoader() {
        return (mapLoader != null);
    }

    public void instrument(NodeEngine nodeEngine) {
        Diagnostics diagnostics = ((NodeEngineImpl) nodeEngine).getDiagnostics();
        StoreLatencyPlugin storeLatencyPlugin = diagnostics.getPlugin(StoreLatencyPlugin.class);
        if (storeLatencyPlugin == null) {
            return;
        }

        if (mapLoader != null) {
            this.mapLoader = new LatencyTrackingMapLoader(mapLoader, storeLatencyPlugin, mapName);
        }

        if (mapStore != null) {
            this.mapStore = new LatencyTrackingMapStore(mapStore, storeLatencyPlugin, mapName);
        }
    }

    @Override
    public void delete(Object key) {
        if (isMapStore()) {
            mapStore.delete(key);
        }
    }

    public void store(Object key, Object value) {
        if (isMapStore()) {
            mapStore.store(key, value);
        }
    }

    @Override
    public void storeAll(Map map) {
        if (isMapStore()) {
            mapStore.storeAll(map);
        }
    }

    @Override
    public void deleteAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        if (isMapStore()) {
            mapStore.deleteAll(keys);
        }
    }

    /**
     * Returns an {@link Iterable} of all keys or {@code null}
     * if a map loader is not configured for this map.
     * {@inheritDoc}
     */
    @Override
    public Iterable<Object> loadAllKeys() {
        if (isMapLoader()) {
            return (Iterable<Object>) mapLoader.loadAllKeys();
        }
        return null;
    }

    @Override
    public Object load(Object key) {
        if (isMapLoader()) {
            return mapLoader.load(key);
        }
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        if (isMapLoader()) {
            return mapLoader.loadAll(keys);
        }
        return null;
    }

    public Object getImpl() {
        return impl;
    }

    public boolean isPostProcessingMapStore() {
        return isMapStore() && mapStore instanceof PostProcessingMapStore;
    }

    public boolean isWithExpirationTime() {
        return withExpirationTime;
    }

    @Override
    public String toString() {
        return "MapStoreWrapper{" + "mapName='" + mapName + '\''
                + ", mapStore=" + mapStore + ", mapLoader=" + mapLoader + '}';
    }
}

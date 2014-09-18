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

package com.hazelcast.map;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.PostProcessingMapStore;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("unchecked")
public class MapStoreWrapper implements MapStore {

    private final MapLoader mapLoader;
    private final MapStore mapStore;
    private final Object impl;
    private final String mapName;
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public MapStoreWrapper(Object impl, String mapName, boolean enabled) {
        this.impl = impl;
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
        this.enabled.set(enabled);
    }


    public MapStore getMapStore() {
        return mapStore;
    }

    public void enable() {
        enabled.set(true);
    }

    public void disable() {
        enabled.set(false);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void destroy() {
        if (impl instanceof MapLoaderLifecycleSupport) {
            ((MapLoaderLifecycleSupport) impl).destroy();
        }
    }

    private boolean isMapStore() {
        return (mapStore != null);
    }

    private boolean isMapLoader() {
        return (mapLoader != null);
    }

    public void delete(Object key) {
        if (isMapStore() && enabled.get()) {
            mapStore.delete(key);
        }
    }

    public void store(Object key, Object value) {
        if (isMapStore() && enabled.get()) {
            mapStore.store(key, value);
        }
    }

    public void storeAll(Map map) {
        if (isMapStore() && enabled.get()) {
            mapStore.storeAll(map);
        }
    }

    public void deleteAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        if (isMapStore() && enabled.get()) {
            mapStore.deleteAll(keys);
        }
    }

    public Set loadAllKeys() {
        if (isMapLoader() && enabled.get()) {
            return mapLoader.loadAllKeys();
        }
        return null;
    }

    public Object load(Object key) {
        if (isMapLoader() && enabled.get()) {
            return mapLoader.load(key);
        }
        return null;
    }

    public Map loadAll(Collection keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        if (isMapLoader() && enabled.get()) {
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

    @Override
    public String toString() {
        return "MapStoreWrapper{" + "mapName='" + mapName + '\''
                + ", mapStore=" + mapStore + ", mapLoader=" + mapLoader + '}';
    }
}

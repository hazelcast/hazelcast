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

package com.hazelcast.map.impl;

import com.hazelcast.config.TenantControl;
import com.hazelcast.config.TenantControl.Closeable;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.PostProcessingMapStore;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.query.impl.getters.ReflectionHelper;
import com.hazelcast.spi.NodeEngine;
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

    private final String mapName;

    private final Object impl;

    private TenantControl tenantControl = new TenantControl.NoTenantControl();

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
        this.mapLoader = loader;
        this.mapStore = store;
    }

    public MapStore getMapStore() {
        return mapStore;
    }

    @Override
    public void destroy() {
        tenantControl.unregister();
        if (impl instanceof MapLoaderLifecycleSupport) {
            ((MapLoaderLifecycleSupport) impl).destroy();
        }
    }

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        tenantControl = hazelcastInstance.getConfig().getTenantControl().saveCurrentTenant(new DestroyEventImpl(mapName));
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
            Iterable<Object> allKeys;
            Closeable tenantContext = tenantControl.setTenant(true);
            try {
                allKeys = mapLoader.loadAllKeys();
            } catch (AbstractMethodError e) {
                // Invoke reflectively to preserve backwards binary compatibility. Removable in v4.x
                allKeys = ReflectionHelper.invokeMethod(mapLoader, "loadAllKeys");
            }
            finally {
                tenantContext.close();
            }
            return allKeys;
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

    @Override
    public String toString() {
        return "MapStoreWrapper{" + "mapName='" + mapName + '\''
                + ", mapStore=" + mapStore + ", mapLoader=" + mapLoader + '}';
    }

    private static class DestroyEventImpl implements TenantControl.DestroyEvent {
        public DestroyEventImpl(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public <TT> void destroy(TT context) {
            HazelcastInstance instance = (HazelcastInstance)context;
            MapService mapService = instance.getDistributedObject(MapService.SERVICE_NAME, mapName);
            // TODO should be remove, not destroy map,
            // but errors remain if a new class tries to load old objects,
            // should be some way to serialize objects here and deserialize them
            // when starting another version of the client
            mapService.getMapServiceContext().removeMapContainer(mapService.getMapServiceContext().getMapContainer(mapName));
        }

        @Override
        public Class<?> getContextType() {
            return HazelcastInstance.class;
        }

        private final String mapName;
        private static final long serialVersionUID = 1L;
    }
}

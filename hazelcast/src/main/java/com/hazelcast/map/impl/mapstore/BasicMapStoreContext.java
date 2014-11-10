/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.map.impl.mapstore.MapStoreManagers.createWriteBehindManager;
import static com.hazelcast.map.impl.mapstore.MapStoreManagers.createWriteThroughManager;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * Default impl. of {@link com.hazelcast.map.impl.mapstore.MapStoreContext}
 */
final class BasicMapStoreContext implements MapStoreContext {

    private static final int INITIAL_KEYS_REMOVE_DELAY_MINUTES = 20;

    private final Map<Data, Object> initialKeys = new ConcurrentHashMap<Data, Object>();

    private MapStoreManager mapStoreManager;

    private MapStoreWrapper storeWrapper;

    private MapContainer mapContainer;

    private BasicMapStoreContext() {
    }

    static MapStoreContext create(MapContainer mapContainer) {
        final BasicMapStoreContext basicMapStoreContext = new BasicMapStoreContext();
        basicMapStoreContext.setMapContainer(mapContainer);

        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final String mapName = mapContainer.getName();
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        final ClassLoader configClassLoader = nodeEngine.getConfigClassLoader();
        final Object store = createStore(mapName, mapStoreConfig, configClassLoader);

        // get writable config (not read-only one) from node engine.
        nodeEngine.getConfig().getMapConfig(mapName).getMapStoreConfig().setImplementation(store);

        final MapStoreWrapper storeWrapper = new MapStoreWrapper(mapName, store);
        basicMapStoreContext.setStoreWrapper(storeWrapper);

        callLifecycleSupportInit(mapName, store, mapStoreConfig, nodeEngine);

        final MapStoreManager mapStoreManager = createMapStoreManager(mapContainer);

        basicMapStoreContext.setMapStoreManager(mapStoreManager);

        return basicMapStoreContext;
    }


    private static MapStoreManager createMapStoreManager(MapContainer mapContainer) {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();

        if (isWriteBehindMapStoreEnabled(mapStoreConfig)) {
            return createWriteBehindManager(mapContainer);
        }
        return createWriteThroughManager(mapContainer);
    }


    private static Object createStore(String name, MapStoreConfig mapStoreConfig, ClassLoader classLoader) {
        // 1. Try to create store from `store factory` class.
        Object store = getStoreFromFactoryOrNull(name, mapStoreConfig, classLoader);

        // 2. Try to get store from `store impl.` object.
        if (store == null) {
            store = getStoreFromImplementationOrNull(mapStoreConfig);
        }
        // 3. Try to create store from `store impl.` class.
        if (store == null) {
            store = getStoreFromClassOrNull(mapStoreConfig, classLoader);
        }
        return store;
    }

    private static Object getStoreFromFactoryOrNull(String name, MapStoreConfig mapStoreConfig, ClassLoader classLoader) {
        MapStoreFactory factory = (MapStoreFactory) mapStoreConfig.getFactoryImplementation();
        if (factory == null) {
            return null;
        }
        final String factoryClassName = mapStoreConfig.getFactoryClassName();
        if (isNullOrEmpty(factoryClassName)) {
            return null;
        }

        try {
            factory = ClassLoaderUtil.newInstance(classLoader, factoryClassName);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (factory == null) {
            return null;
        }

        final Properties properties = mapStoreConfig.getProperties();
        return factory.newMapStore(name, properties);

    }

    private static Object getStoreFromImplementationOrNull(MapStoreConfig mapStoreConfig) {
        return mapStoreConfig.getImplementation();
    }

    private static Object getStoreFromClassOrNull(MapStoreConfig mapStoreConfig, ClassLoader classLoader) {
        Object store;
        String mapStoreClassName = mapStoreConfig.getClassName();
        try {
            store = ClassLoaderUtil.newInstance(classLoader, mapStoreClassName);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return store;
    }

    private static void callLifecycleSupportInit(String mapName, Object store,
                                                 MapStoreConfig mapStoreConfig, NodeEngine nodeEngine) {
        if (!(store instanceof MapLoaderLifecycleSupport)) {
            return;
        }

        final HazelcastInstance hazelcastInstance = nodeEngine.getHazelcastInstance();
        final Properties properties = mapStoreConfig.getProperties();
        final MapLoaderLifecycleSupport mapLoaderLifecycleSupport = (MapLoaderLifecycleSupport) store;

        mapLoaderLifecycleSupport.init(hazelcastInstance, properties, mapName);
    }

    private void loadInitialKeys() {
        final Map<Data, Object> initialKeys = this.initialKeys;
        initialKeys.clear();

        Set keys = storeWrapper.loadAllKeys();
        if (keys == null || keys.isEmpty()) {
            return;
        }

        final int maxSizePerNode = getMaxSizePerNode();
        final MapContainer mapContainer = this.mapContainer;
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();

        for (Object key : keys) {
            Data dataKey = mapServiceContext.toData(key, partitioningStrategy);

            // this node will load only owned keys
            if (mapServiceContext.isOwnedKey(dataKey)) {

                initialKeys.put(dataKey, key);

                if (initialKeys.size() == maxSizePerNode) {
                    break;
                }
            }
        }

        // remove the keys remains more than 20 minutes.
        mapServiceContext.getNodeEngine().getExecutionService().schedule(new Runnable() {
            @Override
            public void run() {
                initialKeys.clear();
            }
        }, INITIAL_KEYS_REMOVE_DELAY_MINUTES, TimeUnit.MINUTES);
    }


    private static boolean isWriteBehindMapStoreEnabled(MapStoreConfig mapStoreConfig) {
        return mapStoreConfig != null && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    /**
     * Get max size per node setting form config
     *
     * @return max size or -1 if policy is not set
     */
    private int getMaxSizePerNode() {
        MaxSizeConfig maxSizeConfig = getMapConfig().getMaxSizeConfig();
        int maxSize = -1;

        if (maxSizeConfig.getMaxSizePolicy() == PER_NODE) {
            maxSize = maxSizeConfig.getSize();
        }

        return maxSize;
    }

    private MapConfig getMapConfig() {
        return mapContainer.getMapConfig();
    }

    public MapStoreManager getMapStoreManager() {
        return mapStoreManager;
    }

    public Map<Data, Object> getInitialKeys() {
        return initialKeys;
    }

    public MapStoreWrapper getStore() {
        return storeWrapper;
    }

    @Override
    public void start() {
        loadInitialKeys();
        mapStoreManager.start();
    }

    @Override
    public void stop() {
        mapStoreManager.stop();
    }

    void setMapStoreManager(MapStoreManager mapStoreManager) {
        this.mapStoreManager = mapStoreManager;
    }

    void setStoreWrapper(MapStoreWrapper storeWrapper) {
        this.storeWrapper = storeWrapper;
    }

    void setMapContainer(MapContainer mapContainer) {
        this.mapContainer = mapContainer;
    }
}

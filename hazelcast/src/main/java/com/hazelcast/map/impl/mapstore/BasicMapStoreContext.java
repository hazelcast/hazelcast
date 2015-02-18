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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.map.impl.eviction.MaxSizeChecker.getApproximateMaxSize;
import static com.hazelcast.map.impl.mapstore.MapStoreManagers.createWriteBehindManager;
import static com.hazelcast.map.impl.mapstore.MapStoreManagers.createWriteThroughManager;
import static com.hazelcast.map.impl.mapstore.StoreConstructor.createStore;

/**
 * Default impl. of {@link com.hazelcast.map.impl.mapstore.MapStoreContext}
 * One instance is created per map.
 */
final class BasicMapStoreContext implements MapStoreContext {

    private static final int INITIAL_KEYS_REMOVE_DELAY_MINUTES = 20;

    private static final String INITIAL_KEY_LOAD_EXECUTOR = "hz:trigger-initial-load";

    private final Map<Data, Object> initialKeys = new ConcurrentHashMap<Data, Object>();

    private String mapName;

    private MapStoreManager mapStoreManager;

    private MapStoreWrapper storeWrapper;

    private MapServiceContext mapServiceContext;

    private PartitioningStrategy partitioningStrategy;

    private MapStoreConfig mapStoreConfig;

    private MaxSizeConfig maxSizeConfig;

    private volatile Future<Boolean> initialKeyLoader;

    private BasicMapStoreContext() {
    }

    @Override
    public void start() {
        mapStoreManager.start();

        final Callable<Boolean> task = new TriggerInitialKeyLoad();
        initialKeyLoader = executeTask(INITIAL_KEY_LOAD_EXECUTOR, task);
    }

    @Override
    public void stop() {
        if (initialKeyLoader != null) {
            initialKeyLoader.cancel(true);
        }
        mapStoreManager.stop();
    }

    @Override
    public boolean isWriteBehindMapStoreEnabled() {
        final MapStoreConfig mapStoreConfig = getMapStoreConfig();
        return mapStoreConfig != null && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    @Override
    public SerializationService getSerializationService() {
        return mapServiceContext.getNodeEngine().getSerializationService();
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return mapServiceContext.getNodeEngine().getLogger(clazz);
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return mapStoreConfig;
    }

    @Override
    public void waitInitialLoadFinish() throws Exception {
        initialKeyLoader.get();
    }

    @Override
    public MapStoreManager getMapStoreManager() {
        return mapStoreManager;
    }

    @Override
    public Map<Data, Object> getInitialKeys() {
        return initialKeys;
    }

    @Override
    public MapStoreWrapper getMapStoreWrapper() {
        return storeWrapper;
    }

    static MapStoreContext create(MapContainer mapContainer) {
        final BasicMapStoreContext context = new BasicMapStoreContext();
        final String mapName = mapContainer.getName();
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        final ClassLoader configClassLoader = nodeEngine.getConfigClassLoader();
        // create store.
        final Object store = createStore(mapName, mapStoreConfig, configClassLoader);
        final MapStoreWrapper storeWrapper = new MapStoreWrapper(mapName, store);

        setStoreImplToWritableMapStoreConfig(nodeEngine, mapName, store);

        context.setMapName(mapName);
        context.setMapStoreConfig(mapStoreConfig);
        context.setMaxSizeConfig(maxSizeConfig);
        context.setPartitioningStrategy(partitioningStrategy);
        context.setMapServiceContext(mapServiceContext);
        context.setStoreWrapper(storeWrapper);

        final MapStoreManager mapStoreManager = createMapStoreManager(context);
        context.setMapStoreManager(mapStoreManager);

        // todo this is user code. it may also block map store creation.
        callLifecycleSupportInit(context);

        return context;
    }

    private static void setStoreImplToWritableMapStoreConfig(NodeEngine nodeEngine, String mapName, Object store) {
        final Config config = nodeEngine.getConfig();
        // get writable config (not read-only one) from node engine.
        final MapConfig mapConfig = config.getMapConfig(mapName);
        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setImplementation(store);
    }


    private static MapStoreManager createMapStoreManager(MapStoreContext mapStoreContext) {
        final MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        if (isWriteBehindMapStoreEnabled(mapStoreConfig)) {
            return createWriteBehindManager(mapStoreContext);
        }
        return createWriteThroughManager(mapStoreContext);
    }

    private static boolean isWriteBehindMapStoreEnabled(MapStoreConfig mapStoreConfig) {
        return mapStoreConfig != null && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    private static void callLifecycleSupportInit(MapStoreContext mapStoreContext) {
        final MapStoreWrapper mapStoreWrapper = mapStoreContext.getMapStoreWrapper();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final HazelcastInstance hazelcastInstance = nodeEngine.getHazelcastInstance();
        final MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        final Properties properties = mapStoreConfig.getProperties();
        final String mapName = mapStoreContext.getMapName();

        mapStoreWrapper.init(hazelcastInstance, properties, mapName);
    }

    private void loadInitialKeys() {
        // load all keys.
        Set keys = storeWrapper.loadAllKeys();
        if (keys == null || keys.isEmpty()) {
            return;
        }

        // select keys owned by current node.
        final MapServiceContext mapServiceContext = getMapServiceContext();
        selectOwnedKeys(keys, mapServiceContext);

        // remove the keys remains more than 20 minutes.
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(new Runnable() {
            @Override
            public void run() {
                initialKeys.clear();
            }
        }, INITIAL_KEYS_REMOVE_DELAY_MINUTES, TimeUnit.MINUTES);
    }

    private void selectOwnedKeys(Set loadedKeys, MapServiceContext mapServiceContext) {
        final Map<Data, Object> initialKeys = this.initialKeys;
        initialKeys.clear();
        final PartitioningStrategy partitioningStrategy = this.partitioningStrategy;
        int maxSizePerNode = getApproximateMaxSize(getMaxSizePerNode()) - 1;

        for (Object key : loadedKeys) {
            Data dataKey = mapServiceContext.toData(key, partitioningStrategy);
            // this node will load only owned keys
            if (mapServiceContext.isOwnedKey(dataKey)) {

                initialKeys.put(dataKey, key);

                if (initialKeys.size() == maxSizePerNode) {
                    break;
                }
            }
        }
    }

    /**
     * Get max size per node setting form config
     *
     * @return max size or -1 if policy is not set
     */
    private int getMaxSizePerNode() {
        final MaxSizeConfig maxSizeConfig = this.maxSizeConfig;
        int maxSize = -1;

        if (maxSizeConfig.getMaxSizePolicy() == PER_NODE) {
            maxSize = maxSizeConfig.getSize();
        }

        return maxSize;
    }

    /**
     * We are offloading initial key load operation.
     * A {@link com.hazelcast.map.impl.RecordStore} should wait finish of this task.
     */
    private final class TriggerInitialKeyLoad implements Callable<Boolean> {
        @Override
        public Boolean call() throws Exception {

            loadInitialKeys();

            return Boolean.TRUE;
        }
    }

    private <T> Future<T> executeTask(String executorName, Callable task) {
        return getExecutionService().submit(executorName, task);

    }

    private ExecutionService getExecutionService() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    void setMapStoreManager(MapStoreManager mapStoreManager) {
        this.mapStoreManager = mapStoreManager;
    }

    void setStoreWrapper(MapStoreWrapper storeWrapper) {
        this.storeWrapper = storeWrapper;
    }

    void setMapServiceContext(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    void setMapName(String mapName) {
        this.mapName = mapName;
    }

    void setPartitioningStrategy(PartitioningStrategy partitioningStrategy) {
        this.partitioningStrategy = partitioningStrategy;
    }

    void setMapStoreConfig(MapStoreConfig mapStoreConfig) {
        this.mapStoreConfig = mapStoreConfig;
    }

    void setMaxSizeConfig(MaxSizeConfig maxSizeConfig) {
        this.maxSizeConfig = maxSizeConfig;
    }
}

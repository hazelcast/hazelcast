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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.mapstore.MapStoreManager;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.DataRecordFactory;
import com.hazelcast.map.record.ObjectRecordFactory;
import com.hazelcast.map.record.OffHeapRecordFactory;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.mapstore.MapStoreManagers.createWriteBehindManager;
import static com.hazelcast.map.mapstore.MapStoreManagers.createWriteThroughManager;
import static com.hazelcast.map.mapstore.MapStoreManagers.emptyMapStoreManager;

/**
 * Map container.
 */
public class MapContainer extends MapContainerSupport {

    private static final int INITIAL_KEYS_REMOVE_DELAY_MINUTES = 20;


    private final String name;
    private final RecordFactory recordFactory;
    private final MapServiceContext mapServiceContext;
    private final List<MapInterceptor> interceptors;
    private final Map<String, MapInterceptor> interceptorMap;
    private final IndexService indexService = new IndexService();
    private final boolean nearCacheEnabled;
    private final SizeEstimator nearCacheSizeEstimator;
    private final PartitioningStrategy partitioningStrategy;
    private WanReplicationPublisher wanReplicationPublisher;
    private MapMergePolicy wanMergePolicy;
    private final Map<Data, Object> initialKeys = new ConcurrentHashMap<Data, Object>();
    private MapStoreWrapper storeWrapper;
    private MapStoreManager mapStoreManager;

    public MapContainer(final String name, final MapConfig mapConfig, final MapServiceContext mapServiceContext) {
        super(mapConfig);
        this.name = name;
        this.mapServiceContext = mapServiceContext;
        this.partitioningStrategy = createPartitioningStrategy();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        recordFactory = createRecordFactory(nodeEngine);
        initMapStoreOperations(nodeEngine);
        initWanReplication(nodeEngine);
        interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        nearCacheEnabled = mapConfig.getNearCacheConfig() != null;
        nearCacheSizeEstimator = SizeEstimators.createNearCacheSizeEstimator();
        mapStoreManager = createMapStoreManager(this);
        mapStoreManager.start();
    }

    public MapStoreManager createMapStoreManager(MapContainer mapContainer) {
        if (!isMapStoreEnabled()) {
            return emptyMapStoreManager();
        }
        if (isWriteBehindMapStoreEnabled()) {
            return createWriteBehindManager(mapContainer);
        } else {
            return createWriteThroughManager(mapContainer);
        }
    }

    public MapStoreManager getMapStoreManager() {
        return mapStoreManager;
    }

    private void initMapStoreOperations(NodeEngine nodeEngine) {
        if (!isMapStoreEnabled()) {
            return;
        }
        storeWrapper = createMapStoreWrapper(mapConfig.getMapStoreConfig(), nodeEngine);
        if (storeWrapper != null) {
            initMapStore(storeWrapper.getImpl(), mapConfig.getMapStoreConfig(), nodeEngine);
        }
    }

    private RecordFactory createRecordFactory(NodeEngine nodeEngine) {
        RecordFactory recordFactory;
        switch (mapConfig.getInMemoryFormat()) {
            case BINARY:
                recordFactory = new DataRecordFactory(mapConfig, nodeEngine.getSerializationService(), partitioningStrategy);
                break;
            case OBJECT:
                recordFactory = new ObjectRecordFactory(mapConfig, nodeEngine.getSerializationService());
                break;
            case OFFHEAP:
                recordFactory = new OffHeapRecordFactory(mapConfig, nodeEngine.getOffHeapStorage(),
                        nodeEngine.getSerializationService(), partitioningStrategy);
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + mapConfig.getInMemoryFormat());
        }
        return recordFactory;
    }

    private MapStoreWrapper createMapStoreWrapper(MapStoreConfig mapStoreConfig, NodeEngine nodeEngine) {
        Object store;
        MapStoreWrapper storeWrapper;
        try {
            MapStoreFactory factory = (MapStoreFactory) mapStoreConfig.getFactoryImplementation();
            if (factory == null) {
                String factoryClassName = mapStoreConfig.getFactoryClassName();
                if (factoryClassName != null && !"".equals(factoryClassName)) {
                    factory = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), factoryClassName);
                }
            }
            store = (factory == null ? mapStoreConfig.getImplementation()
                    : factory.newMapStore(name, mapStoreConfig.getProperties()));
            if (store == null) {
                String mapStoreClassName = mapStoreConfig.getClassName();
                store = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), mapStoreClassName);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        nodeEngine.getConfig().getMapConfig(name).getMapStoreConfig().setImplementation(store);
        storeWrapper = new MapStoreWrapper(store, name, mapStoreConfig.isEnabled());
        return storeWrapper;
    }

    private void initMapStore(Object store, MapStoreConfig mapStoreConfig, NodeEngine nodeEngine) {
        if (store instanceof MapLoaderLifecycleSupport) {
            ((MapLoaderLifecycleSupport) store).init(nodeEngine.getHazelcastInstance(),
                    mapStoreConfig.getProperties(), name);
        }
        loadInitialKeys();
    }


    public void initWanReplication(NodeEngine nodeEngine) {
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return;
        }
        String wanReplicationRefName = wanReplicationRef.getName();
        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        wanReplicationPublisher = wanReplicationService.getWanReplicationPublisher(wanReplicationRefName);
        wanMergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(wanReplicationRef.getMergePolicy());
    }

    private PartitioningStrategy createPartitioningStrategy() {
        PartitioningStrategy strategy = null;
        PartitioningStrategyConfig partitioningStrategyConfig = mapConfig.getPartitioningStrategyConfig();
        if (partitioningStrategyConfig != null) {
            strategy = partitioningStrategyConfig.getPartitioningStrategy();
            if (strategy == null && partitioningStrategyConfig.getPartitioningStrategyClass() != null) {
                try {
                    strategy = ClassLoaderUtil.newInstance(mapServiceContext.getNodeEngine().getConfigClassLoader(),
                            partitioningStrategyConfig.getPartitioningStrategyClass());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        return strategy;
    }

    private void loadInitialKeys() {
        initialKeys.clear();
        Set keys = storeWrapper.loadAllKeys();
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (Object key : keys) {
            Data dataKey = mapServiceContext.toData(key, partitioningStrategy);
            initialKeys.put(dataKey, key);
        }
        // remove the keys remains more than 20 minutes.
        mapServiceContext.getNodeEngine().getExecutionService().schedule(new Runnable() {
            @Override
            public void run() {
                initialKeys.clear();
            }
        }, INITIAL_KEYS_REMOVE_DELAY_MINUTES, TimeUnit.MINUTES);
    }

    public Map<Data, Object> getInitialKeys() {
        return initialKeys;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public WanReplicationPublisher getWanReplicationPublisher() {
        return wanReplicationPublisher;
    }

    public MapMergePolicy getWanMergePolicy() {
        return wanMergePolicy;
    }

    public String addInterceptor(MapInterceptor interceptor) {
        String id = UuidUtil.buildRandomUuidString();
        interceptorMap.put(id, interceptor);
        interceptors.add(interceptor);
        return id;
    }

    public void addInterceptor(String id, MapInterceptor interceptor) {
        interceptorMap.put(id, interceptor);
        interceptors.add(interceptor);
    }

    public List<MapInterceptor> getInterceptors() {
        return interceptors;
    }

    public Map<String, MapInterceptor> getInterceptorMap() {
        return interceptorMap;
    }

    public void removeInterceptor(String id) {
        MapInterceptor interceptor = interceptorMap.remove(id);
        interceptors.remove(interceptor);
    }

    public Record createRecord(Data key, Object value, long ttl, long now) {
        Record record = getRecordFactory().newRecord(key, value);
        record.setLastAccessTime(now);
        record.setLastUpdateTime(now);
        record.setCreationTime(now);
        final long configTTLSeconds = mapConfig.getTimeToLiveSeconds();
        final long configTTLMillis
                = mapServiceContext.convertTime(configTTLSeconds, TimeUnit.SECONDS);

        if (ttl < 0L && configTTLMillis > 0L) {
            record.setTtl(configTTLMillis);
        } else if (ttl > 0L) {
            record.setTtl(ttl);
        }
        return record;
    }

    public String getName() {
        return name;
    }

    public boolean isNearCacheEnabled() {
        return nearCacheEnabled;
    }

    public int getTotalBackupCount() {
        return getBackupCount() + getAsyncBackupCount();
    }

    public int getBackupCount() {
        return mapConfig.getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    public MapStoreWrapper getStore() {
        return storeWrapper != null && storeWrapper.isEnabled() ? storeWrapper : null;
    }

    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    public SizeEstimator getNearCacheSizeEstimator() {
        return nearCacheSizeEstimator;
    }

    public RecordFactory getRecordFactory() {
        return recordFactory;
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }
}

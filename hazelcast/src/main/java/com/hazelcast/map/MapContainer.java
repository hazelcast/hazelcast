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
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.DataRecordFactory;
import com.hazelcast.map.record.ObjectRecordFactory;
import com.hazelcast.map.record.OffHeapRecordFactory;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MapContainer {

    private final String name;
    private volatile MapConfig mapConfig;
    private final RecordFactory recordFactory;
    private final MapService mapService;
    private final MapStoreWrapper storeWrapper;
    private final List<MapInterceptor> interceptors;
    private final Map<String, MapInterceptor> interceptorMap;
    private final IndexService indexService = new IndexService();
    private final boolean nearCacheEnabled;
    private final EntryTaskScheduler idleEvictionScheduler;
    private final EntryTaskScheduler ttlEvictionScheduler;
    private final EntryTaskScheduler mapStoreScheduler;
    private final WanReplicationPublisher wanReplicationPublisher;
    private final MapMergePolicy wanMergePolicy;
    private final SizeEstimator nearCacheSizeEstimator;
    private final Map<Data, Object> initialKeys = new ConcurrentHashMap<Data, Object>();
    private final PartitioningStrategy partitioningStrategy;
    private final String mapStoreScheduledExecutorName;

    public MapContainer(String name, MapConfig mapConfig, MapService mapService) {
        Object store = null;
        this.name = name;
        this.mapConfig = mapConfig;
        this.mapService = mapService;
        this.partitioningStrategy = createPartitioningStrategy();
        this.mapStoreScheduledExecutorName = "hz:scheduled:mapstore:" + name;

        NodeEngine nodeEngine = mapService.getNodeEngine();
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

        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (mapStoreConfig != null && mapStoreConfig.isEnabled()) {
            try {
                MapStoreFactory factory = (MapStoreFactory) mapStoreConfig.getFactoryImplementation();
                if (factory == null) {
                    String factoryClassName = mapStoreConfig.getFactoryClassName();
                    if (factoryClassName != null && !"".equals(factoryClassName)) {
                        factory = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), factoryClassName);
                    }
                }
                store = (factory == null ? mapStoreConfig.getImplementation() :
                        factory.newMapStore(name, mapStoreConfig.getProperties()));
                if (store == null) {
                    String mapStoreClassName = mapStoreConfig.getClassName();
                    store = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), mapStoreClassName);
                }
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
            storeWrapper = new MapStoreWrapper(store, name, mapStoreConfig.isEnabled());
        } else {
            storeWrapper = null;
        }

        if (storeWrapper != null) {
            if (store instanceof MapLoaderLifecycleSupport) {
                ((MapLoaderLifecycleSupport) store).init(nodeEngine.getHazelcastInstance(),
                        mapStoreConfig.getProperties(), name);
            }
            loadInitialKeys();

            if (mapStoreConfig.getWriteDelaySeconds() > 0) {
                final ExecutionService executionService = nodeEngine.getExecutionService();
                executionService.register(mapStoreScheduledExecutorName, 1, 100000, ExecutorType.CACHED);
                ScheduledExecutorService scheduledExecutor = executionService
                        .getScheduledExecutor(mapStoreScheduledExecutorName);
                mapStoreScheduler = EntryTaskSchedulerFactory.newScheduler(scheduledExecutor,
                        new MapStoreProcessor(this, mapService),
                        ScheduleType.FOR_EACH);
            } else {
                mapStoreScheduler = null;
            }
        } else {
            mapStoreScheduler = null;
        }
        ScheduledExecutorService defaultScheduledExecutor = nodeEngine.getExecutionService()
                .getDefaultScheduledExecutor();
        ttlEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(defaultScheduledExecutor,
                new EvictionProcessor(nodeEngine, mapService, name), ScheduleType.POSTPONE);
        idleEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(defaultScheduledExecutor,
                new EvictionProcessor(nodeEngine, mapService, name), ScheduleType.POSTPONE);

        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef != null) {
            this.wanReplicationPublisher = nodeEngine.getWanReplicationService().getWanReplicationListener(
                    wanReplicationRef.getName());
            this.wanMergePolicy = mapService.getMergePolicy(wanReplicationRef.getMergePolicy());
        } else {
            wanMergePolicy = null;
            wanReplicationPublisher = null;
        }

        interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        nearCacheEnabled = mapConfig.getNearCacheConfig() != null;
        nearCacheSizeEstimator = SizeEstimators.createNearCacheSizeEstimator();
    }

    private PartitioningStrategy createPartitioningStrategy() {
        PartitioningStrategy strategy = null;
        PartitioningStrategyConfig partitioningStrategyConfig = mapConfig.getPartitioningStrategyConfig();
        if (partitioningStrategyConfig != null) {
            strategy = partitioningStrategyConfig.getPartitioningStrategy();
            if (strategy == null && partitioningStrategyConfig.getPartitioningStrategyClass() != null) {
                try {
                    strategy = ClassLoaderUtil.newInstance(mapService.getNodeEngine().getConfigClassLoader(),
                            partitioningStrategyConfig.getPartitioningStrategyClass());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        return strategy;
    }

    public void loadInitialKeys() {
        initialKeys.clear();
        Set keys = storeWrapper.loadAllKeys();
        if (keys == null || keys.isEmpty()) {
            return;
        }
        for (Object key : keys) {
            Data dataKey = mapService.toData(key, partitioningStrategy);
            initialKeys.put(dataKey, key);
        }
        // remove the keys remains more than 20 minutes.
        mapService.getNodeEngine().getExecutionService().schedule(new Runnable() {
            @Override
            public void run() {
                initialKeys.clear();
            }
        }, 20, TimeUnit.MINUTES);
    }

    public void shutDownMapStoreScheduledExecutor() {
        mapService.getNodeEngine().getExecutionService().shutdownExecutor(mapStoreScheduledExecutorName);
    }

    public Map<Data, Object> getInitialKeys() {
        return initialKeys;
    }

    public EntryTaskScheduler getIdleEvictionScheduler() {
        return idleEvictionScheduler;
    }

    public EntryTaskScheduler getTtlEvictionScheduler() {
        return ttlEvictionScheduler;
    }

    public EntryTaskScheduler getMapStoreScheduler() {
        return mapStoreScheduler;
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

    public long getWriteDelayMillis() {
        return TimeUnit.SECONDS.toMillis(mapConfig.getMapStoreConfig().getWriteDelaySeconds());
    }

    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    public MapConfig getMapConfig() {
        return mapConfig;
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

    public MapService getMapService() {
        return mapService;
    }
}

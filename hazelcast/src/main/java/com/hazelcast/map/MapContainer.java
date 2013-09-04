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
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.MapInitialLoadOperation;
import com.hazelcast.map.operation.MapIsReadyOperation;
import com.hazelcast.map.operation.PutFromLoadOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.MapService.SERVICE_NAME;

public class MapContainer {

    private final String name;
    private final MapConfig mapConfig;
    private final MapService mapService;
    private final MapStoreWrapper storeWrapper;
    private final List<MapInterceptor> interceptors;
    private final Map<String, MapInterceptor> interceptorMap;
    private final IndexService indexService = new IndexService();
    private final boolean nearCacheEnabled;
    private final AtomicBoolean initialLoaded = new AtomicBoolean(false);
    private final EntryTaskScheduler idleEvictionScheduler;
    private final EntryTaskScheduler ttlEvictionScheduler;
    private final EntryTaskScheduler mapStoreWriteScheduler;
    private final EntryTaskScheduler mapStoreDeleteScheduler;
    private final WanReplicationPublisher wanReplicationPublisher;
    private final MapMergePolicy wanMergePolicy;
    private volatile boolean mapReady = false;

    public MapContainer(String name, MapConfig mapConfig, MapService mapService) {
        Object store = null;
        this.name = name;
        this.mapConfig = mapConfig;
        this.mapService = mapService;
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        NodeEngine nodeEngine = mapService.getNodeEngine();

        if (mapStoreConfig != null) {
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
            storeWrapper = new MapStoreWrapper(store, mapConfig.getName(), mapStoreConfig.isEnabled());
        } else {
            storeWrapper = null;
        }

        if (storeWrapper != null) {
            if (store instanceof MapLoaderLifecycleSupport) {
                ((MapLoaderLifecycleSupport) store).init(nodeEngine.getHazelcastInstance(), mapStoreConfig.getProperties(), name);
            }
            // only master can initiate the loadAll. master will send other members to loadAll.
            // the members join later will not load from mapstore.
            if (nodeEngine.getClusterService().isMaster() && initialLoaded.compareAndSet(false, true)) {
                loadMapFromStore(true);
                Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
                for (Member member : members) {
                    try {
                        if (member.localMember())
                            continue;
                        MemberImpl memberImpl = (MemberImpl) member;
                        Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, new MapInitialLoadOperation(name), memberImpl.getAddress()).build();
                        invocation.invoke();
                    } catch (Throwable t) {
                        throw ExceptionUtil.rethrow(t);
                    }
                }
            } else {
                try {
                    Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, new MapIsReadyOperation(name), nodeEngine.getMasterAddress()).build();
                    Future future = invocation.invoke();
                    mapReady = (Boolean) future.get();
                    while (!mapReady) {
                        Thread.sleep(1000);
                        invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, new MapIsReadyOperation(name), nodeEngine.getMasterAddress()).build();
                        future = invocation.invoke();
                        boolean temp = (Boolean) future.get();
                        if(!mapReady) {
                            mapReady = temp;
                        }
                    }
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }

            if (mapStoreConfig.getWriteDelaySeconds() > 0) {
                mapStoreWriteScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(), new MapStoreWriteProcessor(this, mapService), ScheduleType.FOR_EACH);
                mapStoreDeleteScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(), new MapStoreDeleteProcessor(this, mapService), ScheduleType.SCHEDULE_IF_NEW);
            } else {
                mapStoreDeleteScheduler = null;
                mapStoreWriteScheduler = null;
            }
        } else {
            mapReady = true;
            mapStoreDeleteScheduler = null;
            mapStoreWriteScheduler = null;
        }
        ttlEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(), new EvictionProcessor(nodeEngine, mapService, name), ScheduleType.POSTPONE);
        idleEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(), new EvictionProcessor(nodeEngine, mapService, name), ScheduleType.POSTPONE);

        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef != null) {
            this.wanReplicationPublisher = nodeEngine.getWanReplicationService().getWanReplicationListener(wanReplicationRef.getName());
            this.wanMergePolicy = mapService.getMergePolicy(wanReplicationRef.getMergePolicy());
        } else {
            wanMergePolicy = null;
            wanReplicationPublisher = null;
        }

        interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        nearCacheEnabled = mapConfig.getNearCacheConfig() != null;
    }

    public boolean isMapReady() {
        // map ready states whether the map load operation has been finished. if not retry exception is sent.
        return mapReady;
    }

    public void loadMapFromStore(boolean force) {
        if (force || initialLoaded.compareAndSet(false, true)) {
            mapReady = false;
            NodeEngine nodeEngine = mapService.getNodeEngine();
            int chunkSize = nodeEngine.getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
            Set keys = storeWrapper.loadAllKeys();
            if (keys == null || keys.isEmpty()) {
                mapReady = true;
                return;
            }
            Map<Data, Object> chunk = new HashMap<Data, Object>();


            List<Map<Data, Object>> chunkList = new ArrayList<Map<Data, Object>>();
            for (Object key : keys) {
                Data dataKey = mapService.toData(key);
                int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
                Address partitionOwner = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
                while (partitionOwner == null) {
                    partitionOwner = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                }
                if (partitionOwner.equals(nodeEngine.getClusterService().getThisAddress())) {
                    chunk.put(dataKey, key);
                    if (chunk.size() >= chunkSize) {
                        chunkList.add(chunk);
                        chunk = new HashMap<Data, Object>();
                    }
                }
            }
            if (chunk.size() > 0) {
                chunkList.add(chunk);
            }
            int numberOfChunks = chunkList.size();
            AtomicInteger counter = new AtomicInteger(numberOfChunks);
            for (Map<Data, Object> currentChunk : chunkList) {
                try {
                    nodeEngine.getExecutionService().submit("hz:map-load", new MapLoadAllTask(currentChunk, counter));
                } catch (Throwable t) {
                    ExceptionUtil.rethrow(t);
                }
            }

        }
    }

    public EntryTaskScheduler getIdleEvictionScheduler() {
        return idleEvictionScheduler;
    }

    public EntryTaskScheduler getTtlEvictionScheduler() {
        return ttlEvictionScheduler;
    }

    public EntryTaskScheduler getMapStoreWriteScheduler() {
        return mapStoreWriteScheduler;
    }

    public EntryTaskScheduler getMapStoreDeleteScheduler() {
        return mapStoreDeleteScheduler;
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
        String id = UUID.randomUUID().toString();
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
        return mapConfig.getMapStoreConfig().getWriteDelaySeconds() * 1000;
    }

    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public MapStoreWrapper getStore() {
        return storeWrapper;
    }

    private class MapLoadAllTask implements Runnable {
        private Map<Data, Object> keys;
        private AtomicInteger counter;

        private MapLoadAllTask(Map<Data, Object> keys, AtomicInteger counter) {
            this.keys = keys;
            this.counter = counter;
        }

        public void run() {
            NodeEngine nodeEngine = mapService.getNodeEngine();
            Map values = storeWrapper.loadAll(keys.values());
            final CountDownLatch latch = new CountDownLatch(keys.size());
            for (Data dataKey : keys.keySet()) {
                Object key = keys.get(dataKey);
                Data dataValue = mapService.toData(values.get(key));
                int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
                PutFromLoadOperation operation = new PutFromLoadOperation(name, dataKey, dataValue, -1);
                operation.setNodeEngine(nodeEngine);
                operation.setResponseHandler(new ResponseHandler() {
                    @Override
                    public void sendResponse(Object obj) {
                        latch.countDown();
                    }
                });
                operation.setPartitionId(partitionId);
                OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
                operation.setServiceName(MapService.SERVICE_NAME);
                nodeEngine.getOperationService().executeOperation(operation);
            }

            try {
                if (latch.await(30, TimeUnit.SECONDS) && counter.decrementAndGet() <= 0) {
                    mapReady = true;
                }
            } catch (InterruptedException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

    }

}

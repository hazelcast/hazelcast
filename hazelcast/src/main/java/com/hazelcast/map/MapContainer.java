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
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.MapOperationsCounter;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.map.MapService.SERVICE_NAME;

public class MapContainer {

    private final String name;
    private final MapConfig mapConfig;
    private final MapService mapService;
    private final MapStore store;
    // TODO: do we really need to store interceptors in 3 separate collections?
    // TODO: at first phase you can remove the ability to removeInterceptor
    private final List<MapInterceptor> interceptors;
    private final Map<String, MapInterceptor> interceptorMap;
    private final Map<MapInterceptor, String> interceptorIdMap;
    private final IndexService indexService = new IndexService();
    private final boolean nearCacheEnabled;
    private final MapOperationsCounter mapOperationCounter = new MapOperationsCounter();
    private volatile boolean mapReady = false;
    private final long creationTime;
    private final AtomicBoolean initialLoaded = new AtomicBoolean(false);

    public MapContainer(String name, MapConfig mapConfig, MapService mapService) {
        MapStore storeTemp = null;
        this.name = name;
        this.mapConfig = mapConfig;
        this.mapService = mapService;
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();

        if (mapStoreConfig != null) {
            try {
                MapStoreFactory factory = (MapStoreFactory) mapStoreConfig.getFactoryImplementation();
                if (factory == null) {
                    String factoryClassName = mapStoreConfig.getFactoryClassName();
                    if (factoryClassName != null && !"".equals(factoryClassName)) {
                        factory = (MapStoreFactory) ClassLoaderUtil.newInstance(factoryClassName);
                    }
                }
                storeTemp = (MapStore) (factory == null ? mapStoreConfig.getImplementation() :
                        factory.newMapStore(name, mapStoreConfig.getProperties()));
                if (storeTemp == null) {
                    String mapStoreClassName = mapStoreConfig.getClassName();
                    storeTemp = ClassLoaderUtil.newInstance(mapStoreClassName);
                }
            } catch (Exception e) {
                ExceptionUtil.rethrow(e);
                storeTemp = null;
            }
        }

        store = storeTemp;

        if (store != null) {
            NodeEngine nodeEngine = mapService.getNodeEngine();
            // only master can initiate the loadAll. master will send other members to loadAll.
            // the members join later will not load from mapstore.
            if (nodeEngine.getClusterService().isMaster() && initialLoaded.compareAndSet(false, true)) {
                loadMapFromStore(true);
                Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
                Operation operation = new MapInitialLoadOperation(name);
                for (Member member : members) {
                    try {
                        if (member.localMember())
                            continue;
                        MemberImpl memberImpl = (MemberImpl) member;
                        Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, memberImpl.getAddress()).build();
                        invocation.invoke();
                    } catch (Throwable t) {
                        throw ExceptionUtil.rethrow(t);
                    }
                }
            } else {
                mapReady = true;
            }
        } else {
            mapReady = true;
        }
        interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        interceptorIdMap = new ConcurrentHashMap<MapInterceptor, String>();
        nearCacheEnabled = mapConfig.getNearCacheConfig() != null;
        creationTime = Clock.currentTimeMillis();
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
            Set keys = store.loadAllKeys();
            if (keys == null || keys.isEmpty()) {
                mapReady = true;
                return;
            }
            Map<Data, Object> chunk = new HashMap<Data, Object>();


            List<Map<Data,Object>> chunkList = new ArrayList<Map<Data, Object>>();
            for (Object key : keys) {
                Data dataKey = mapService.toData(key);
                int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
                if (nodeEngine.getPartitionService().getPartitionOwner(partitionId).equals(nodeEngine.getClusterService().getThisAddress())) {
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
                nodeEngine.getExecutionService().submit("hz:map-load-all", new MapLoadAllTask(currentChunk, counter));
                } catch (Throwable t) {
                    ExceptionUtil.rethrow(t);
                }
            }

        }
    }

    private class MapLoadAllTask implements Runnable {
        private Map<Data, Object> keys;
        private AtomicInteger counter;

        private MapLoadAllTask(Map<Data, Object> keys, AtomicInteger counter) {
            this.keys = keys;
            this.counter = counter;
        }

        @Override
        public void run() {
            NodeEngine nodeEngine = mapService.getNodeEngine();
            Map values = store.loadAll(keys.values());
            for (Data dataKey : keys.keySet()) {
                Object key = keys.get(dataKey);
                Data dataValue = mapService.toData(values.get(key));
                int partitionId = nodeEngine.getPartitionService().getPartitionId(dataKey);
                PutFromLoadOperation operation = new PutFromLoadOperation(name, dataKey, dataValue, null, -1);
                operation.setNodeEngine(nodeEngine);
                operation.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                operation.setPartitionId(partitionId);
                operation.setServiceName(MapService.SERVICE_NAME);
                try {
                    nodeEngine.getOperationService().runOperation(operation);
                } catch (Throwable t) {
                    ExceptionUtil.rethrow(t);
                }
            }

            if (counter.decrementAndGet() <= 0) {
                mapReady = true;
            }
        }
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public MapOperationsCounter getMapOperationCounter() {
        return mapOperationCounter;
    }

    public String addInterceptor(MapInterceptor interceptor) {
        String id = "interceptor" + UUID.randomUUID();
        interceptorMap.put(id, interceptor);
        interceptorIdMap.put(interceptor, id);
        interceptors.add(interceptor);
        return id;
    }

    public void addInterceptor(MapInterceptor interceptor, String id) {
        interceptorMap.put(id, interceptor);
        interceptorIdMap.put(interceptor, id);
        interceptors.add(interceptor);
    }

    public List<MapInterceptor> getInterceptors() {
        return interceptors;
    }

    public String removeInterceptor(MapInterceptor interceptor) {
        String id = interceptorIdMap.remove(interceptor);
        interceptorMap.remove(id);
        interceptors.remove(interceptor);
        return id;
    }

    public void removeInterceptor(String id) {
        MapInterceptor interceptor = interceptorMap.remove(id);
        interceptorIdMap.remove(interceptor);
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

    public MapStore getStore() {
        return store;
    }

    public long getCreationTime() {
        return creationTime;
    }
}

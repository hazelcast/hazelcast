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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.NativeRecordFactory;
import com.hazelcast.map.impl.record.ObjectRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.map.impl.ExpirationTimeSetter.pickTTL;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTime;
import static com.hazelcast.map.impl.SizeEstimators.createNearCacheSizeEstimator;
import static com.hazelcast.map.impl.mapstore.MapStoreContextFactory.createMapStoreContext;

/**
 * Map container.
 */
public class MapContainer extends MapContainerSupport {

    private final RecordFactory recordFactory;

    private final MapServiceContext mapServiceContext;

    private final List<MapInterceptor> interceptors;

    private final Map<String, MapInterceptor> interceptorMap;

    private final IndexService indexService = new IndexService();

    private final SizeEstimator nearCacheSizeEstimator;

    private final PartitioningStrategy partitioningStrategy;

    private final MapStoreContext mapStoreContext;

    private WanReplicationPublisher wanReplicationPublisher;

    private MapMergePolicy wanMergePolicy;

    public MapContainer(final String name, final MapConfig mapConfig, final MapServiceContext mapServiceContext) {
        super(name, mapConfig);
        this.mapServiceContext = mapServiceContext;
        this.partitioningStrategy = createPartitioningStrategy();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        recordFactory = createRecordFactory(nodeEngine);
        initWanReplication(nodeEngine);
        interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        nearCacheSizeEstimator = createNearCacheSizeEstimator();
        mapStoreContext = createMapStoreContext(this);

    }

    public void init() {
        mapStoreContext.start();
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
            case NATIVE:
                recordFactory = new NativeRecordFactory(mapConfig, nodeEngine.getOffHeapStorage(),
                        nodeEngine.getSerializationService(), partitioningStrategy);
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + mapConfig.getInMemoryFormat());
        }
        return recordFactory;
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
        String id = interceptor.getClass().getName() + interceptor.hashCode();

        addInterceptor(id, interceptor);

        return id;
    }

    public void addInterceptor(String id, MapInterceptor interceptor) {

        removeInterceptor(id);

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

    public Record createRecord(Data key, Object value, long ttlMillis, long now) {
        Record record = getRecordFactory().newRecord(key, value);
        record.setLastAccessTime(now);
        record.setLastUpdateTime(now);
        record.setCreationTime(now);

        final long ttlMillisFromConfig = getTtlMillisFromConfig();
        final long ttl = pickTTL(ttlMillis, ttlMillisFromConfig);
        record.setTtl(ttl);

        final long maxIdleMillis = getMaxIdleMillis();
        setExpirationTime(record, maxIdleMillis);
        return record;
    }


    public boolean isNearCacheEnabled() {
        return mapConfig.isNearCacheEnabled();
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

    public MapStoreContext getMapStoreContext() {
        return mapStoreContext;
    }
}



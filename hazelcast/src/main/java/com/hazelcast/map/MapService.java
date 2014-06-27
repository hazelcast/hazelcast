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

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStoreInfo;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.eviction.ExpirationManager;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.MergeOperation;
import com.hazelcast.map.operation.PostJoinMapOperation;
import com.hazelcast.map.operation.WanOriginatedDeleteOperation;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.map.tx.TransactionalMapProxy;
import com.hazelcast.map.wan.MapReplicationRemove;
import com.hazelcast.map.wan.MapReplicationUpdate;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * The SPI Service for the Map.
 */
public class MapService extends MapServiceSupport implements ManagedService, TransactionalService, RemoteService,
        PostJoinAwareService, ReplicationSupportingService {

    /**
     * Service name.
     */
    public static final String SERVICE_NAME = "hz:impl:mapService";
    private final ExpirationManager expirationManager;
    private final NearCacheProvider nearCacheProvider;
    private final LocalMapStatsProvider localMapStatsProvider;

    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            return new MapContainer(mapName, nodeEngine.getConfig().findMapConfig(mapName), MapService.this);
        }
    };

    public MapService(NodeEngine nodeEngine) {
        super(nodeEngine);
        expirationManager = new ExpirationManager(this);
        nearCacheProvider = new NearCacheProvider(this);
        localMapStatsProvider = new LocalMapStatsProvider(this);
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(SERVICE_NAME, new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
                public LockStoreInfo createNew(final ObjectNamespace key) {
                    final MapContainer mapContainer = getMapContainer(key.getObjectName());
                    return new LockStoreInfo() {
                        public int getBackupCount() {
                            return mapContainer.getBackupCount();
                        }

                        public int getAsyncBackupCount() {
                            return mapContainer.getAsyncBackupCount();
                        }
                    };
                }
            });
        }
        expirationManager.start();
    }

    @Override
    public void reset() {
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.clear();
            }
        }
        nearCacheProvider.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            flushMapsBeforeShutdown();
            destroyMapStores();
            final PartitionContainer[] containers = partitionContainers;
            for (PartitionContainer container : containers) {
                if (container != null) {
                    container.clear();
                }
            }
            nearCacheProvider.clear();
            mapContainers.clear();
        }
    }

    private void destroyMapStores() {
        for (MapContainer mapContainer : mapContainers.values()) {
            MapStoreWrapper store = mapContainer.getStore();
            if (store != null) {
                store.destroy();
            }
        }
    }

    private void flushMapsBeforeShutdown() {
        for (PartitionContainer partitionContainer : partitionContainers) {
            for (String mapName : mapContainers.keySet()) {
                RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                recordStore.setLoaded(true);
                recordStore.flush();
            }
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        PostJoinMapOperation o = new PostJoinMapOperation();
        for (MapContainer mapContainer : mapContainers.values()) {
            o.addMapIndex(mapContainer);
            o.addMapInterceptors(mapContainer);
        }
        return o;
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (eventObject instanceof MapReplicationUpdate) {
            MapReplicationUpdate replicationUpdate = (MapReplicationUpdate) eventObject;
            EntryView entryView = replicationUpdate.getEntryView();
            MapMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
            String mapName = replicationUpdate.getMapName();
            MapContainer mapContainer = getMapContainer(mapName);
            MergeOperation operation = new MergeOperation(mapName, toData(entryView.getKey(),
                    mapContainer.getPartitioningStrategy()), entryView, mergePolicy);
            try {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(entryView.getKey());
                Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        } else if (eventObject instanceof MapReplicationRemove) {
            MapReplicationRemove replicationRemove = (MapReplicationRemove) eventObject;
            WanOriginatedDeleteOperation operation = new WanOriginatedDeleteOperation(replicationRemove.getMapName(),
                    replicationRemove.getKey());
            try {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationRemove.getKey());
                Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }


    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, mapContainers, mapConstructor);
    }

    @Override
    protected MapService getService() {
        return this;
    }


    @SuppressWarnings("unchecked")
    @Override
    public TransactionalMapProxy createTransactionalObject(String name, TransactionSupport transaction) {
        return new TransactionalMapProxy(name, this, nodeEngine, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {

    }

    @Override
    public MapProxyImpl createDistributedObject(String name) {
        return new MapProxyImpl(name, this, nodeEngine);
    }

    @Override
    public void destroyDistributedObject(String name) {
        MapContainer mapContainer = mapContainers.remove(name);
        if (mapContainer != null) {
            if (mapContainer.isNearCacheEnabled()) {
                nearCacheProvider.remove(name);
            }
            mapContainer.getMapStoreManager().stop();
        }
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.destroyMap(name);
            }
        }
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
    }

    public NearCacheProvider getNearCacheProvider() {
        return nearCacheProvider;
    }

    public LocalMapStatsProvider getLocalMapStatsProvider() {
        return localMapStatsProvider;
    }
}

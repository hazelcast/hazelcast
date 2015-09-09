/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.map.impl.eviction.EvictionOperator;
import com.hazelcast.map.impl.eviction.ExpirationManager;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.operation.MapPartitionDestroyOperation;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Collections;
import java.util.LinkedHashSet;

import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Default implementation of map service context.
 */
class MapServiceContextImpl implements MapServiceContext {
    private static final long DESTROY_TIMEOUT_SECONDS = 30;

    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final ConcurrentMap<String, MapContainer> mapContainers;
    private final AtomicReference<Collection<Integer>> ownedPartitions;
    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            final MapServiceContext mapServiceContext = getService().getMapServiceContext();
            final Config config = nodeEngine.getConfig();
            final MapConfig mapConfig = config.findMapConfig(mapName);
            final MapContainer mapContainer = new MapContainer(mapName, mapConfig, mapServiceContext);
            return mapContainer;
        }
    };
    /**
     * Per node global write behind queue item counter.
     * Creating here because we want to have a counter per node.
     * This is used by owner and backups together so it should be defined
     * getting this into account.
     */
    private final AtomicInteger writeBehindQueueItemCounter = new AtomicInteger(0);
    private final ExpirationManager expirationManager;
    private final NearCacheProvider nearCacheProvider;
    private final LocalMapStatsProvider localMapStatsProvider;
    private final MergePolicyProvider mergePolicyProvider;
    private final MapContextQuerySupport mapContextQuerySupport;
    private MapEventPublisher mapEventPublisher;
    private EvictionOperator evictionOperator;
    private MapService mapService;

    public MapServiceContextImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.partitionContainers = new PartitionContainer[partitionCount];
        this.mapContainers = new ConcurrentHashMap<String, MapContainer>();
        this.ownedPartitions = new AtomicReference<Collection<Integer>>();
        this.expirationManager = new ExpirationManager(this, nodeEngine);
        this.evictionOperator = EvictionOperator.create(this);
        this.nearCacheProvider = new NearCacheProvider(this, nodeEngine);
        this.localMapStatsProvider = new LocalMapStatsProvider(this, nodeEngine);
        this.mergePolicyProvider = new MergePolicyProvider(nodeEngine);
        this.mapEventPublisher = createMapEventPublisherSupport();
        this.mapContextQuerySupport = new BasicMapContextQuerySupport(this);
    }

    MapEventPublisherImpl createMapEventPublisherSupport() {
        return new MapEventPublisherImpl(this);
    }

    @Override
    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, mapContainers, mapConstructor);
    }

    @Override
    public Map<String, MapContainer> getMapContainers() {
        return mapContainers;
    }

    @Override
    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    @Override
    public void initPartitionsContainers() {
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final PartitionContainer[] partitionContainers = this.partitionContainers;
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(getService(), i);
        }
    }

    @Override
    public void clearPartitionData(int partitionId) {
        final PartitionContainer container = partitionContainers[partitionId];
        if (container != null) {
            for (RecordStore mapPartition : container.getMaps().values()) {
                mapPartition.clearPartition();
            }
            container.getMaps().clear();
        }
    }

    @Override
    public MapService getService() {
        return mapService;
    }

    @Override
    public void clearPartitions() {
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.clear();
            }
        }
    }

    @Override
    public void destroyMapStores() {
        for (MapContainer mapContainer : mapContainers.values()) {
            MapStoreWrapper store = mapContainer.getMapStoreContext().getMapStoreWrapper();
            if (store != null) {
                store.destroy();
            }
        }
    }

    @Override
    public void flushMaps() {
        for (PartitionContainer partitionContainer : partitionContainers) {
            for (String mapName : mapContainers.keySet()) {
                RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                recordStore.flush();
            }
        }
    }

    @Override
    public void destroyMap(String mapName) {
        final PartitionContainer[] containers = partitionContainers;
        final List<Future> futures = new ArrayList<Future>(containers.length);
        for (PartitionContainer container : containers) {
            if (container != null) {
                int partitionId = container.getPartitionId();
                InternalPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);

                if (partition.isLocal()) {
                    OperationService operationService = nodeEngine.getOperationService();
                    Operation operation = new MapPartitionDestroyOperation(container, mapName);

                    Future f = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
                    futures.add(f);
                }
            }
        }

        try {
            for (Future f : futures) {
                f.get(DESTROY_TIMEOUT_SECONDS,
                        TimeUnit.SECONDS);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void reset() {
        clearPartitions();
        getNearCacheProvider().clear();
    }

    @Override
    public NearCacheProvider getNearCacheProvider() {
        return nearCacheProvider;
    }

    @Override
    public RecordStore getRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getRecordStore(mapName);
    }

    @Override
    public RecordStore getExistingRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getExistingRecordStore(mapName);
    }

    @Override
    public Collection<Integer> getOwnedPartitions() {
        Collection<Integer> partitions = ownedPartitions.get();
        if (partitions == null) {
            reloadOwnedPartitions();
            partitions = ownedPartitions.get();
        }
        return partitions;
    }

    @Override
    public void reloadOwnedPartitions() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Collection<Integer> partitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        if (partitions == null) {
            partitions = Collections.emptySet();
        }
        ownedPartitions.set(Collections.unmodifiableSet(new LinkedHashSet<Integer>(partitions)));
    }

    @Override
    public AtomicInteger getWriteBehindQueueItemCounter() {
        return writeBehindQueueItemCounter;
    }

    @Override
    public ExpirationManager getExpirationManager() {
        return expirationManager;
    }

    @Override
    public EvictionOperator getEvictionOperator() {
        return evictionOperator;
    }

    @Override
    public void setService(MapService mapService) {
        this.mapService = mapService;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public MergePolicyProvider getMergePolicyProvider() {
        return mergePolicyProvider;
    }

    @Override
    public MapEventPublisher getMapEventPublisher() {
        return mapEventPublisher;
    }

    @Override
    public MapContextQuerySupport getMapContextQuerySupport() {
        return mapContextQuerySupport;
    }

    @Override
    public LocalMapStatsProvider getLocalMapStatsProvider() {
        return localMapStatsProvider;
    }

    /**
     * Used for testing purposes.
     */
    @Override
    public void setEvictionOperator(EvictionOperator evictionOperator) {
        this.evictionOperator = evictionOperator;
    }

    @Override
    public Object toObject(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    @Override
    public Data toData(Object object, PartitioningStrategy partitionStrategy) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object, partitionStrategy);
        }
    }

    @Override
    public Data toData(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    @Override
    public boolean compare(String mapName, Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null) {
            return false;
        }
        if (value2 == null) {
            return false;
        }
        final MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.getRecordFactory().isEquals(value1, value2);
    }

    @Override
    public void interceptAfterGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            value = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterGet(value);
            }
        }
    }

    @Override
    public Object interceptPut(String mapName, Object oldValue, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(newValue);
            oldValue = toObject(oldValue);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptPut(oldValue, result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? newValue : result;
    }

    @Override
    public void interceptAfterPut(String mapName, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            newValue = toObject(newValue);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterPut(newValue);
            }
        }
    }

    @Override
    public Object interceptRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptRemove(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    @Override
    public void interceptAfterRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            for (MapInterceptor interceptor : interceptors) {
                value = toObject(value);
                interceptor.afterRemove(value);
            }
        }
    }

    @Override
    public void addInterceptor(String id, String mapName, MapInterceptor interceptor) {
        MapContainer mapContainer = getMapContainer(mapName);
        mapContainer.addInterceptor(id, interceptor);
    }


    @Override
    public String generateInterceptorId(String mapName, MapInterceptor interceptor) {
        return interceptor.getClass().getName() + interceptor.hashCode();
    }

    @Override
    public void removeInterceptor(String mapName, String id) {
        getMapContainer(mapName).removeInterceptor(id);
    }

    // todo interceptors should get a wrapped object which includes the serialized version
    @Override
    public Object interceptGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptGet(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    @Override
    public boolean hasInterceptor(String mapName) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        return !interceptors.isEmpty();
    }

    @Override
    public String addLocalEventListener(Object listener, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(SERVICE_NAME, mapName, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addLocalEventListener(Object listener, EventFilter eventFilter, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventRegistration registration = nodeEngine.getEventService().
                registerLocalListener(SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addEventListener(Object listener, EventFilter eventFilter, String mapName) {
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventRegistration registration = nodeEngine.getEventService().
                registerListener(SERVICE_NAME, mapName, eventFilter, listenerAdaptor);
        return registration.getId();
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener, String mapName) {
        final ListenerAdapter listenerAdapter = new InternalMapPartitionLostListenerAdapter(listener);
        final EventFilter filter = new MapPartitionLostEventFilter();
        final EventRegistration registration = nodeEngine.getEventService().registerListener(SERVICE_NAME, mapName, filter,
                listenerAdapter);
        return registration.getId();
    }

    @Override
    public boolean removeEventListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public boolean removePartitionLostListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, mapName, registrationId);
    }
}

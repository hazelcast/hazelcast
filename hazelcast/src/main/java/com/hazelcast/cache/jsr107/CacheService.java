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

package com.hazelcast.cache.jsr107;


import com.hazelcast.cache.jsr107.operation.CacheReplicationOperation;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.EventType;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * @author mdogan 05/02/14
 */
public class CacheService implements ManagedService, RemoteService, MigrationAwareService
        , EventPublishingService<CacheEventData, CacheEventListenerAdaptor> {

    public final static String SERVICE_NAME = "hz:impl:cacheService";
    private ILogger logger;
    private NodeEngine nodeEngine;
    private CachePartitionSegment[] segments;
    private ConcurrentMap<CacheEntryListenerConfiguration, EventRegistration> eventRegistrationMap;

    //region ManagedService
    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = new CachePartitionSegment(nodeEngine, this, i);
        }
        eventRegistrationMap = new ConcurrentHashMap<CacheEntryListenerConfiguration, EventRegistration>();
    }

    @Override
    public void reset() {
        final CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                partitionSegment.clear();
            }
        }
//        for (NearCache nearCache : nearCacheMap.values()) {
//            nearCache.clear();
//        }

    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            //flushWriteCacheBeforeShutdown();
            //destroyMapStores();
            this.reset();
        }
    }
    //endregion


    //region RemoteService
    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new CacheProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteCache(objectName);
        }
    }
    //endregion


    //region MigrationAwareService
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {/*empty*/}

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionReplica(event.getPartitionId());
        }

    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        segments[partitionId].clear();
    }
    //endregion


    //region CacheService Impls
    public CachePartitionSegment getSegment(int partitionId) {
        return segments[partitionId];
    }

    public ICacheRecordStore getOrCreateCache(String name, int partitionId) {
        return segments[partitionId].getOrCreateCache(name);
    }

    public ICacheRecordStore getCache(String name, int partitionId) {
        return segments[partitionId].getCache(name);
    }

    public Object toObject(Object data) {
        if (data == null)
            return null;
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    public Data toData(Object object) {
        if (object == null)
            return null;
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    public void publishEvent(String cacheName, EventType eventType, Data dataKey, Object oldValue, Object value) {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> candidates = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }

        Set<EventRegistration> registrationsWithOldValue = new HashSet<EventRegistration>();
        Set<EventRegistration> registrationsWithoutOldValue = new HashSet<EventRegistration>();

        Object objectValue = toObject(value);
        Object objectOldValue = toObject(oldValue);


        for (EventRegistration candidate : candidates) {
            EventFilter filter = candidate.getFilter();

            final Object key = toObject(dataKey);
            if (filter instanceof CacheEventFilterAdaptor) {
                final CacheEventFilterAdaptor<Object, Object> ceFilter = (CacheEventFilterAdaptor<Object, Object>) filter;
                if (ceFilter.filterEventData(eventType, key, objectValue, objectOldValue)) {
                    if (ceFilter.isOldValueRequired()) {
                        registrationsWithOldValue.add(candidate);
                    } else {
                        registrationsWithoutOldValue.add(candidate);
                    }
                }
            }
        }
        if (registrationsWithOldValue.isEmpty() && registrationsWithoutOldValue.isEmpty()) {
            return;
        }

        Data dataValue = toData(value);
        Data dataOldValue = toData(oldValue);
        ;
        if (eventType == EventType.REMOVED || eventType == EventType.EXPIRED) {
            dataValue = dataValue != null ? dataValue : dataOldValue;
        }

//        final Address caller=null;
        int orderKey = dataKey.hashCode();
        CacheEventData eventWithOldValue = new CacheEventData(cacheName, dataKey, dataValue, dataOldValue, eventType);
        CacheEventData eventWithOutOldValue = new CacheEventData(cacheName, dataKey, dataValue, null, eventType);

        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithOldValue, eventWithOldValue, orderKey);
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithoutOldValue, eventWithOutOldValue, orderKey);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void dispatchEvent(CacheEventData eventData, CacheEventListenerAdaptor listener) {
        //Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());

        final EventType eventType = eventData.getEventType();
        final Object key = toObject(eventData.getDataKey());
        final Object newValue = toObject(eventData.getDataNewValue());
        final Object oldValue = toObject(eventData.getDataOldValue());
        listener.handleEvent(nodeEngine, eventData.getName(), eventType, key, newValue, oldValue);
    }

    public <K, V> void registerCacheEntryListener(CacheProxy<K, V> cacheProxy, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final CacheEventFilterAdaptor<K, V> eventFilter = new CacheEventFilterAdaptor<K, V>(cacheProxy, cacheEntryListenerConfiguration);
        final CacheEventListenerAdaptor<K, V> entryListener = new CacheEventListenerAdaptor<K, V>(cacheProxy, cacheEntryListenerConfiguration);

        final EventService eventService = getNodeEngine().getEventService();
        final EventRegistration registration = eventService.registerListener(CacheService.SERVICE_NAME, cacheProxy.getName(), eventFilter, entryListener);

        eventRegistrationMap.put(cacheEntryListenerConfiguration, registration);
    }

    public <K, V> void deregisterCacheEntryListener(CacheProxy<K, V> cacheProxy, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final EventRegistration eventRegistration = eventRegistrationMap.remove(cacheEntryListenerConfiguration);
        if (eventRegistration != null) {
            final EventService eventService = getNodeEngine().getEventService();
            eventService.deregisterListener(SERVICE_NAME, cacheProxy.getName(), eventRegistration.getId());
        }
    }

    //endregion


}

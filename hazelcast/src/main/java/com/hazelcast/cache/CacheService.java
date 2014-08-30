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

package com.hazelcast.cache;

import com.hazelcast.cache.operation.CacheReplicationOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
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
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache Service
 */
public class CacheService
        implements ManagedService, RemoteService, MigrationAwareService,
                   EventPublishingService<Object, CacheEventListener> {

    /**
     * Service name
     */
    public static final String SERVICE_NAME = "hz:impl:cacheService";
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();
    private final ConcurrentMap<String, CacheStatistics> statistics = new ConcurrentHashMap<String, CacheStatistics>();
    private ILogger logger;
    private NodeEngine nodeEngine;
    private CachePartitionSegment[] segments;
    private ConcurrentMap<String, Map<CacheEntryListenerConfiguration, EventRegistration>> eventRegistrationMap;

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
        eventRegistrationMap = new ConcurrentHashMap<String, Map<CacheEntryListenerConfiguration, EventRegistration>>();
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
        return new CacheDistributedObject(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteCache(objectName);
        }
        deregisterAllListener(objectName);
        enableStatistics(objectName, false);
        enableManagement(objectName, false);

        deleteCacheConfig(objectName);
        deleteCacheStat(objectName);
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
    public void beforeMigration(PartitionMigrationEvent event) { /*empty*/ }

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

    public ICacheRecordStore getOrCreateCache(String name, int partitionId) {
        return segments[partitionId].getOrCreateCache(name);
    }

    public ICacheRecordStore getCache(String name, int partitionId) {
        return segments[partitionId].getCache(name);
    }

    public boolean createCacheConfigIfAbsent(CacheConfig config) {
        final CacheConfig localConfig = configs.putIfAbsent(config.getNameWithPrefix(), config);
        return localConfig == null;
    }

    public boolean updateCacheConfig(CacheConfig config) {
        final CacheConfig oldConfig = configs.put(config.getNameWithPrefix(), config);
        return oldConfig != null;
    }

    public void deleteCacheConfig(String name) {
        configs.remove(name);
    }

    public CacheStatistics createCacheStatIfAbsent(String name) {
        if (!statistics.containsKey(name)) {
            statistics.putIfAbsent(name, new CacheStatistics());
        }
        return statistics.get(name);
    }

    public void deleteCacheStat(String name) {
        statistics.remove(name);
    }

    public void enableStatistics(String cacheNameWithPrefix, boolean enabled) {
        final CacheConfig cacheConfig = configs.get(cacheNameWithPrefix);
        if (cacheConfig != null) {
            cacheConfig.setStatisticsEnabled(enabled);
            if (enabled) {
                final CacheStatistics cacheStatistics = createCacheStatIfAbsent(cacheNameWithPrefix);
                final CacheStatisticsMXBeanImpl mxBean = new CacheStatisticsMXBeanImpl(cacheStatistics);

                MXBeanUtil.registerCacheObject(mxBean, cacheConfig.getUriString(), cacheConfig.getName(), true);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheConfig.getUriString(), cacheConfig.getName(), true);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    public void enableManagement(String cacheNameWithPrefix, boolean enabled) {
        final CacheConfig cacheConfig = configs.get(cacheNameWithPrefix);
        if (cacheConfig != null) {
            cacheConfig.setManagementEnabled(enabled);
            if (enabled) {
                final CacheMXBeanImpl mxBean = new CacheMXBeanImpl(cacheConfig);
                MXBeanUtil.registerCacheObject(mxBean, cacheConfig.getUriString(), cacheConfig.getName(), false);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheConfig.getUriString(), cacheConfig.getName(), false);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    public CacheConfig getCacheConfig(String name) {
        return configs.get(name);
    }

    public Iterable<String> getCacheNames() {
        return configs.keySet();
    }

    public Collection<CacheConfig> getCacheConfigs() {
        return configs.values();
    }

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

    public void publishEvent(String cacheName, CacheEventType eventType, Data dataKey, Data dataValue, Data dataOldValue,
                             boolean isOldValueAvailable, int orderKey) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        final Object eventData;
        switch (eventType){
            case CREATED:
            case UPDATED:
            case REMOVED:
            case EXPIRED:
                final CacheEventData cacheEventData = new CacheEventDataImpl(cacheName, eventType, dataKey, dataValue, dataOldValue,isOldValueAvailable);
                CacheEventSet eventSet = new CacheEventSet(eventType);
                eventSet.addEventData(cacheEventData);
                eventData = eventSet;
                break;
            case EVICTED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.EVICTED, dataKey, null, null, false);
                break;
            case INVALIDATED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.INVALIDATED, dataKey, null, null, false);
                break;
            case COMPLETED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.COMPLETED, dataKey, dataValue, null, false);
                break;
            default:
                throw new IllegalArgumentException("Event Type not defined to create an eventData during publish : " + eventType.name());
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventData, orderKey);
    }

    public void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventSet, orderKey);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void dispatchEvent(Object event, CacheEventListener listener) {
        listener.handleEvent(event);

    }

    public String registerListener(String distributedObjectName, CacheEventListener listener) {
        final EventService eventService = getNodeEngine().getEventService();
        final EventRegistration registration = eventService
                .registerListener(CacheService.SERVICE_NAME, distributedObjectName, listener);
        return registration.getId();
    }

    public boolean deregisterListener(String name, String registrationId) {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.deregisterListener(SERVICE_NAME, name, registrationId);
    }

    public void deregisterAllListener(String name) {
        nodeEngine.getEventService().deregisterAllListeners(CacheService.SERVICE_NAME, name);
    }

    //endregion

}

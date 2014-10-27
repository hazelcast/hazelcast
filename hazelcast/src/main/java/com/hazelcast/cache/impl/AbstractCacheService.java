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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheOperationProvider;
import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class AbstractCacheService implements ICacheService {

    protected final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();
    protected final ConcurrentMap<String, CacheStatisticsImpl> statistics = new ConcurrentHashMap<String, CacheStatisticsImpl>();
    protected NodeEngine nodeEngine;
    protected CachePartitionSegment[] segments;

    //region ManagedService
    @Override
    public final void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = new CachePartitionSegment(this, i);
        }
    }

    protected abstract ICacheRecordStore createNewRecordStore(String name, int partitionId);
    //endregion

    //region RemoteService
    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new CacheDistributedObject(objectName, nodeEngine, this);
    }

    //    @Override
    public void destroyDistributedObject(String objectName) {
    }
    //endregion

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

    //region CacheService Impls

    @Override
    public ICacheRecordStore getOrCreateCache(String name, int partitionId) {
        return segments[partitionId].getOrCreateCache(name);
    }

    @Override
    public ICacheRecordStore getCacheRecordStore(String name, int partitionId) {
        return segments[partitionId].getCache(name);
    }

    @Override
    public CachePartitionSegment getSegment(int partitionId) {
        return segments[partitionId];
    }

    @Override
    public void destroyCache(String objectName, boolean isLocal, String callerUuid) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteCache(objectName);
        }
        if (!isLocal) {
            deregisterAllListener(objectName);
        }
        setStatisticsEnabled(objectName, false);
        setManagementEnabled(objectName, false);
        deleteCacheConfig(objectName);
        deleteCacheStat(objectName);
        if (!isLocal) {
            destroyCacheOnAllMembers(objectName, callerUuid);
        }
    }

    protected void destroyCacheOnAllMembers(String objectName, String callerUuid) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (!member.localMember() && !member.getUuid().equals(callerUuid)) {
                final CacheDestroyOperation op = new CacheDestroyOperation(objectName, true);
                operationService.invokeOnTarget(AbstractCacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    @Override
    public boolean createCacheConfigIfAbsent(CacheConfig config, boolean isLocal) {
        final CacheConfig localConfig = configs.putIfAbsent(config.getNameWithPrefix(), config);
        final boolean created = localConfig == null;
        if (created) {
            if (config.isStatisticsEnabled()) {
                setStatisticsEnabled(config.getNameWithPrefix(), true);
            }
            if (config.isManagementEnabled()) {
                setManagementEnabled(config.getNameWithPrefix(), true);
            }
            if (!isLocal) {
                createConfigOnAllMembers(config);
            }
        }
        return created;
    }

    protected <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                final CacheCreateConfigOperation op = new CacheCreateConfigOperation(cacheConfig, true);
                operationService.invokeOnTarget(AbstractCacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    @Override
    public void deleteCacheConfig(String name) {
        configs.remove(name);
    }

    @Override
    public CacheStatisticsImpl createCacheStatIfAbsent(String name) {
        if (!statistics.containsKey(name)) {
            statistics.putIfAbsent(name, new CacheStatisticsImpl());
        }
        return statistics.get(name);
    }

    @Override
    public void deleteCacheStat(String name) {
        statistics.remove(name);
    }

    @Override
    public void setStatisticsEnabled(String cacheNameWithPrefix, boolean enabled) {
        final CacheConfig cacheConfig = configs.get(cacheNameWithPrefix);
        if (cacheConfig != null) {
            final String cacheManagerName = cacheConfig.getUriString();
            cacheConfig.setStatisticsEnabled(enabled);
            if (enabled) {
                final CacheStatisticsImpl cacheStatistics = createCacheStatIfAbsent(cacheNameWithPrefix);
                final CacheStatisticsMXBeanImpl mxBean = new CacheStatisticsMXBeanImpl(cacheStatistics);

                MXBeanUtil.registerCacheObject(mxBean, cacheManagerName, cacheConfig.getName(), true);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheManagerName, cacheConfig.getName(), true);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    @Override
    public void setManagementEnabled(String cacheNameWithPrefix, boolean enabled) {
        final CacheConfig cacheConfig = configs.get(cacheNameWithPrefix);
        if (cacheConfig != null) {
            final String cacheManagerName = cacheConfig.getUriString();
            cacheConfig.setManagementEnabled(enabled);
            if (enabled) {
                final CacheMXBeanImpl mxBean = new CacheMXBeanImpl(cacheConfig);
                MXBeanUtil.registerCacheObject(mxBean, cacheManagerName, cacheConfig.getName(), false);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheManagerName, cacheConfig.getName(), false);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    @Override
    public CacheConfig getCacheConfig(String name) {
        return configs.get(name);
    }

    @Override
    public Collection<CacheConfig> getCacheConfigs() {
        return configs.values();
    }

    @Override
    public void publishEvent(String cacheName, CacheEventType eventType, Data dataKey, Data dataValue,
                             Data dataOldValue, boolean isOldValueAvailable, int orderKey) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> candidates =
                eventService.getRegistrations(AbstractCacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        final Object eventData;
        switch (eventType) {
            case CREATED:
            case UPDATED:
            case REMOVED:
            case EXPIRED:
                final CacheEventData cacheEventData = new CacheEventDataImpl(cacheName, eventType, dataKey, dataValue,
                        dataOldValue, isOldValueAvailable);
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
                throw new IllegalArgumentException(
                        "Event Type not defined to create an eventData during publish : " + eventType.name());
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventData, orderKey);
    }

    @Override
    public void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> candidates =
                eventService.getRegistrations(AbstractCacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventSet, orderKey);
    }

    @Override
    public void dispatchEvent(Object event, CacheEventListener listener) {
        listener.handleEvent(event);
    }

    @Override
    public String registerListener(String distributedObjectName, CacheEventListener listener) {
        final EventService eventService = nodeEngine.getEventService();
        final EventRegistration registration = eventService
                .registerListener(AbstractCacheService.SERVICE_NAME, distributedObjectName, listener);
        return registration.getId();
    }

    @Override
    public boolean deregisterListener(String name, String registrationId) {
        final EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, name, registrationId);
    }

    @Override
    public void deregisterAllListener(String name) {
        nodeEngine.getEventService().deregisterAllListeners(AbstractCacheService.SERVICE_NAME, name);
    }

    @Override
    public CacheStatisticsImpl getStatistics(String name) {
        return statistics.get(name);
    }

    //endregion

    @Override
    public CacheOperationProvider getCacheOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.OFFHEAP.equals(inMemoryFormat)) {
            throw new IllegalArgumentException("OffHeap is available in Enterprise!!!");
        }
        return new DefaultOperationProvider(nameWithPrefix);
    }

}

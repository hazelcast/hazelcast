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

import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.cache.impl.operation.CacheGetConfigOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class AbstractCacheService implements ICacheService {

    protected final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();
    protected final ConcurrentMap<String, CacheStatisticsImpl> statistics = new ConcurrentHashMap<String, CacheStatisticsImpl>();
        protected final ConcurrentMap<String, CacheOperationProvider> operationProviderCache =
            new ConcurrentHashMap<String, CacheOperationProvider>();
    protected final ConstructorFunction<String, CacheStatisticsImpl> cacheStatisticsConstructorFunction =
            new ConstructorFunction<String, CacheStatisticsImpl>() {
                @Override
                public CacheStatisticsImpl createNew(String name) {
                    return new CacheStatisticsImpl();
                }
            };

    protected NodeEngine nodeEngine;
    protected CachePartitionSegment[] segments;

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

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new CacheDistributedObject(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

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

    protected void destroySegments(String objectName) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteCache(objectName);
        }
    }

    @Override
    public void destroyCache(String objectName, boolean isLocal, String callerUuid) {
        CacheConfig config = deleteCacheConfig(objectName);
        destroySegments(objectName);

        if (!isLocal) {
            deregisterAllListener(objectName);
        }
        operationProviderCache.remove(objectName);
        setStatisticsEnabled(config, objectName, false);
        setManagementEnabled(config, objectName, false);
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
    public CacheConfig createCacheConfigIfAbsent(CacheConfig config) {
        final CacheConfig localConfig = configs.putIfAbsent(config.getNameWithPrefix(), config);
        if (localConfig == null) {
            if (config.isStatisticsEnabled()) {
                setStatisticsEnabled(config, config.getNameWithPrefix(), true);
            }
            if (config.isManagementEnabled()) {
                setManagementEnabled(config, config.getNameWithPrefix(), true);
            }
        }
        return localConfig;
    }

    @Override
    public CacheConfig deleteCacheConfig(String name) {
        return configs.remove(name);
    }

    @Override
    public CacheStatisticsImpl createCacheStatIfAbsent(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statistics, name, cacheStatisticsConstructorFunction);
    }

    @Override
    public void deleteCacheStat(String name) {
        statistics.remove(name);
    }

    @Override
    public void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled) {
        cacheConfig = cacheConfig != null ? cacheConfig : configs.get(cacheNameWithPrefix);
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
    public void setManagementEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled) {
        cacheConfig = cacheConfig != null ? cacheConfig : configs.get(cacheNameWithPrefix);
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
    public CacheSimpleConfig findCacheConfig(String simpleName) {
        if (simpleName == null) {
            return null;
        }
        return nodeEngine.getConfig().findCacheConfig(simpleName);
    }

    protected <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheNameWithPrefix, String cacheName) {
        //remote check
        final CacheGetConfigOperation op = new CacheGetConfigOperation(cacheNameWithPrefix, cacheName);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(cacheNameWithPrefix);
        final InternalCompletableFuture<CacheConfig> f = nodeEngine.getOperationService()
                .invokeOnPartition(CacheService.SERVICE_NAME, op, partitionId);
        return f.getSafely();
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

    @Override
    public void publishEvent(String cacheName, CacheEventType eventType, Data dataKey, Data dataValue,
                             Data dataOldValue, boolean isOldValueAvailable, int orderKey, int completionId) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(SERVICE_NAME, cacheName);

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
                CacheEventSet eventSet = new CacheEventSet(eventType, completionId);
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
                CacheEventData completedEventData = new CacheEventDataImpl(cacheName, CacheEventType.COMPLETED,
                        dataKey, dataValue, null, false);
                eventSet = new CacheEventSet(eventType, completionId);
                eventSet.addEventData(completedEventData);
                eventData = eventSet;
                break;
            default:
                throw new IllegalArgumentException(
                        "Event Type not defined to create an eventData during publish : " + eventType.name());
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventData, orderKey);
    }

    @Override
    public void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventSet, orderKey);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void dispatchEvent(Object event, CacheEventListener listener) {
        listener.handleEvent(event);
    }

    @Override
    public String registerListener(String distributedObjectName, CacheEventListener listener) {
        final EventService eventService = getNodeEngine().getEventService();
        final EventRegistration registration = eventService
                .registerListener(AbstractCacheService.SERVICE_NAME, distributedObjectName, listener);
        return registration.getId();
    }

    @Override
    public boolean deregisterListener(String name, String registrationId) {
        final EventService eventService = getNodeEngine().getEventService();
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

    @Override
    public CacheOperationProvider getCacheOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.NATIVE.equals(inMemoryFormat)) {
            throw new IllegalArgumentException("Native memory is available only in Enterprise!");
        }
        CacheOperationProvider cacheOperationProvider = operationProviderCache.get(nameWithPrefix);
        if (cacheOperationProvider != null) {
            return cacheOperationProvider;
        }
        cacheOperationProvider = new DefaultOperationProvider(nameWithPrefix);
        CacheOperationProvider current = operationProviderCache.putIfAbsent(nameWithPrefix, cacheOperationProvider);
        return current == null ? cacheOperationProvider : current;
    }
}

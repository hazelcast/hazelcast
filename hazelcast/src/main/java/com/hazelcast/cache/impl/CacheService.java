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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.operation.CacheCreateConfigOperation;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache Service is the main access point of JCache implementation.
 * <p>
 * This service is responsible for:
 *<ul>
 *     <li>Creating and/or accessing the named {@link com.hazelcast.cache.impl.CacheRecordStore}.</li>
 *     <li>Creating/Deleting the cache configuration of the named {@link com.hazelcast.cache.ICache}.</li>
 *     <li>Registering/Deregistering of cache listeners.</li>
 *     <li>Publish/dispatch cache events.</li>
 *     <li>Enabling/Disabling statistic and management.</li>
 *     <li>Data migration commit/rollback through {@link MigrationAwareService}.</li>
 *</ul>
 * </p>
 * <p><b>WARNING:</b>This service is an optionally registered service which is enabled when {@link javax.cache.Caching}
 * class is found on the classpath.</p>
 * <p>
 * If registered, it will provide all the above cache operations for all partitions of the node which it
 * is registered on.
 * </p>
 * <p><b>Distributed Cache Name</b> is used for providing a unique name to a cache object to overcome cache manager
 * scoping which depends on URI and class loader parameters. It's a simple concatenation of CacheNamePrefix and
 * cache name where CacheNamePrefix is calculated by each cache manager
 * using {@link AbstractHazelcastCacheManager#cacheNamePrefix()}.
 * </p>
 */
public class CacheService
        implements ManagedService, RemoteService, MigrationAwareService, EventPublishingService<Object, CacheEventListener> {

    /**
     * Cache service name literal.
     */
    public static final String SERVICE_NAME = "hz:impl:cacheService";
    private final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();
    private final ConcurrentMap<String, CacheStatisticsImpl> statistics = new ConcurrentHashMap<String, CacheStatisticsImpl>();
    private NodeEngine nodeEngine;
    //todo visibility guarantee?
    private CachePartitionSegment[] segments;

    //region ManagedService
    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = new CachePartitionSegment(nodeEngine, this, i);
        }
    }

    @Override
    public void reset() {
        for (String objectName : configs.keySet()) {
            destroyCache(objectName, true, null);
        }
        final CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                partitionSegment.clear();
            }
        }
        //TODO: near cache not implemented yet. enable wen ready
        //        for (NearCache nearCache : nearCacheMap.values()) {
        //            nearCache.clear();
        //        }

    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
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

    /**
     * Creates or gets the {@link ICacheRecordStore} via internal {@link CachePartitionSegment} using cache
     * name and partitionId.
     * @param name cache name.
     * @param partitionId partition id of the cache.
     * @return {@link ICacheRecordStore}.
     * @see CachePartitionSegment
     * @see ICacheRecordStore
     */
    public ICacheRecordStore getOrCreateCache(String name, int partitionId) {
        return segments[partitionId].getOrCreateCache(name);
    }

    /**
     *
     * Gets the {@link ICacheRecordStore} via internal {@link CachePartitionSegment} using cache name and partitionId.
     * @param name cache name.
     * @param partitionId partition id of the cache.
     * @return {@link ICacheRecordStore}  or null if not created yet.
     * @see CachePartitionSegment
     * @see ICacheRecordStore
     */
    public ICacheRecordStore getCache(String name, int partitionId) {
        return segments[partitionId].getCache(name);
    }

    /**
     * Destroys the internal content, configuration and releases all resources of a cache from all partitions on
     * all nodes.
     *
     * <p>Note: This operation deletes cache from the cluster as if not created before. The caller node won't
     * be destroyed. Once destroy operations on all other nodes are completed, then the caller node's cache will be
     * destroyed.</p>
     * @param objectName distributed cache name.
     * @param isLocal if true, destroys on this node only.
     * @param callerUuid the uuid of the node that called this method.
     */
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

    private void destroyCacheOnAllMembers(String objectName, String callerUuid) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (!member.localMember() && !member.getUuid().equals(callerUuid)) {
                final CacheDestroyOperation op = new CacheDestroyOperation(objectName, true);
                //todo exception handling?
                operationService.invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    /**
     * Creates the cache configuration on the cluster if not created previously.
     * @param config cache configuration to be created.
     * @param isLocal creates on current node only if true.
     * @return is it created or not.
     */
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

    private <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                final CacheCreateConfigOperation op = new CacheCreateConfigOperation(cacheConfig, true);
                //todo exception handling?
                operationService.invokeOnTarget(CacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    /**
     * Removes the cache configuration with the provided name.
     * @param name distributed cache name.
     */
    public void deleteCacheConfig(String name) {
        configs.remove(name);
    }

    /**
     * Creates the cache statistics with provided cache name.
     * @param name distributed cache name.
     * @return {@link CacheStatisticsImpl} or an empty statistics if not enabled.
     */
    public CacheStatisticsImpl createCacheStatIfAbsent(String name) {
        CacheStatisticsImpl statistics = new CacheStatisticsImpl();
        CacheStatisticsImpl temp = this.statistics.putIfAbsent(name, statistics);
        if (temp != null) {
            statistics = temp;
        }
        return statistics;
    }

    public void deleteCacheStat(String name) {
        statistics.remove(name);
    }

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

    public CacheConfig getCacheConfig(String name) {
        return configs.get(name);
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

    void publishEvent(String cacheName, CacheEventType eventType, Data dataKey, Data dataValue, Data dataOldValue,
                             boolean isOldValueAvailable, int orderKey) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);

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

    void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventSet, orderKey);
    }

    NodeEngine getNodeEngine() {
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

    public CacheStatisticsImpl getStatistics(String name) {
        return statistics.get(name);
    }

    //endregion

}

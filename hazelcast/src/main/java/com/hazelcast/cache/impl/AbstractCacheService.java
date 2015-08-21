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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.cache.impl.operation.PostJoinCacheOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Member;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListener;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class AbstractCacheService
        implements ICacheService, PostJoinAwareService, PartitionAwareService, QuorumAwareService {

    protected final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();
    protected final ConcurrentMap<String, CacheContext> cacheContexts = new ConcurrentHashMap<String, CacheContext>();
    protected final ConcurrentMap<String, CacheStatisticsImpl> statistics = new ConcurrentHashMap<String, CacheStatisticsImpl>();
    protected final ConcurrentMap<String, Set<Closeable>> resources = new ConcurrentHashMap<String, Set<Closeable>>();
    protected final ConcurrentMap<String, Closeable> closeableListeners = new ConcurrentHashMap<String, Closeable>();
    protected final ConcurrentMap<String, CacheOperationProvider> operationProviderCache =
            new ConcurrentHashMap<String, CacheOperationProvider>();
    protected final ConstructorFunction<String, CacheContext> cacheContexesConstructorFunction =
            new ConstructorFunction<String, CacheContext>() {
                @Override
                public CacheContext createNew(String name) {
                    return new CacheContext();
                }
            };
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
        postInit(nodeEngine, properties);
    }

    protected void postInit(NodeEngine nodeEngine, Properties properties) { };

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
    public ICacheRecordStore getOrCreateRecordStore(String name, int partitionId) {
        return segments[partitionId].getOrCreateRecordStore(name);
    }

    @Override
    public ICacheRecordStore getRecordStore(String name, int partitionId) {
        return segments[partitionId].getRecordStore(name);
    }

    @Override
    public CachePartitionSegment getSegment(int partitionId) {
        return segments[partitionId];
    }

    protected void destroySegments(String name) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteRecordStore(name);
        }
    }

    @Override
    public void destroyCache(String name, boolean isLocal, String callerUuid) {
        CacheConfig config = deleteCacheConfig(name);
        destroySegments(name);

        if (!isLocal) {
            deregisterAllListener(name);
            cacheContexts.remove(name);
        }
        operationProviderCache.remove(name);
        setStatisticsEnabled(config, name, false);
        setManagementEnabled(config, name, false);
        deleteCacheConfig(name);
        deleteCacheStat(name);
        deleteCacheResources(name);
        if (!isLocal) {
            destroyCacheOnAllMembers(name, callerUuid);
        }
    }

    protected void destroyCacheOnAllMembers(String name, String callerUuid) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            if (!member.localMember() && !member.getUuid().equals(callerUuid)) {
                final CacheDestroyOperation op = new CacheDestroyOperation(name, true);
                operationService.invokeOnTarget(AbstractCacheService.SERVICE_NAME, op, member.getAddress());
            }
        }
    }

    @Override
    public CacheConfig putCacheConfigIfAbsent(CacheConfig config) {
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

    public CacheContext getCacheContext(String name) {
        return cacheContexts.get(name);
    }

    public CacheContext getOrCreateCacheContext(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(cacheContexts, name, cacheContexesConstructorFunction);
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
    public void publishEvent(CacheEventContext cacheEventContext) {
        final EventService eventService = getNodeEngine().getEventService();
        final String cacheName = cacheEventContext.getCacheName();
        final Collection<EventRegistration> candidates = eventService.getRegistrations(SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        final Object eventData;
        final CacheEventType eventType = cacheEventContext.getEventType();
        switch (eventType) {
            case CREATED:
            case UPDATED:
            case REMOVED:
            case EXPIRED:
                final CacheEventData cacheEventData =
                        new CacheEventDataImpl(cacheName, eventType, cacheEventContext.getDataKey(),
                                               cacheEventContext.getDataValue(), cacheEventContext.getDataOldValue(),
                                               cacheEventContext.isOldValueAvailable());
                CacheEventSet eventSet = new CacheEventSet(eventType, cacheEventContext.getCompletionId());
                eventSet.addEventData(cacheEventData);
                eventData = eventSet;
                break;
            case EVICTED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.EVICTED,
                                                   cacheEventContext.getDataKey(), null, null, false);
                break;
            case INVALIDATED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.INVALIDATED,
                                                   cacheEventContext.getDataKey(), null, null, false);
                break;
            case COMPLETED:
                CacheEventData completedEventData =
                        new CacheEventDataImpl(cacheName, CacheEventType.COMPLETED, cacheEventContext.getDataKey(),
                                               cacheEventContext.getDataValue(), null, false);
                eventSet = new CacheEventSet(eventType, cacheEventContext.getCompletionId());
                eventSet.addEventData(completedEventData);
                eventData = eventSet;
                break;
            default:
                throw new IllegalArgumentException(
                        "Event Type not defined to create an eventData during publish : " + eventType.name());
        }
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, candidates, eventData, cacheEventContext.getOrderKey());
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
    public String registerListener(String name, CacheEventListener listener) {
        return registerListenerInternal(name, listener, null);
    }

    @Override
    public String registerListener(String name, CacheEventListener listener, EventFilter eventFilter) {
        return registerListenerInternal(name, listener, eventFilter);
    }

    protected String registerListenerInternal(String name, CacheEventListener listener, EventFilter eventFilter) {
        final EventService eventService = getNodeEngine().getEventService();
        final EventRegistration registration;
        if (eventFilter == null) {
            registration = eventService.registerListener(AbstractCacheService.SERVICE_NAME, name, listener);
        } else {
            registration = eventService.registerListener(AbstractCacheService.SERVICE_NAME, name, eventFilter, listener);
        }
        final String id = registration.getId();
        if (listener instanceof Closeable) {
            closeableListeners.put(id, (Closeable) listener);
        } else if (listener instanceof CacheEntryListenerProvider) {
            CacheEntryListener cacheEntryListener = ((CacheEntryListenerProvider) listener).getCacheEntryListener();
            if (cacheEntryListener instanceof Closeable) {
                closeableListeners.put(id, (Closeable) cacheEntryListener);
            }
        }
        return id;
    }

    @Override
    public boolean deregisterListener(String name, String registrationId) {
        final EventService eventService = getNodeEngine().getEventService();
        boolean result = eventService.deregisterListener(SERVICE_NAME, name, registrationId);
        Closeable listener = closeableListeners.remove(registrationId);
        if (listener != null) {
            IOUtil.closeResource(listener);
        }
        return result;
    }

    @Override
    public void deregisterAllListener(String name) {
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (registrations != null) {
            for (EventRegistration registration : registrations) {
                Closeable listener = closeableListeners.remove(registration.getId());
                if (listener != null) {
                    IOUtil.closeResource(listener);
                }
            }
        }
        eventService.deregisterAllListeners(AbstractCacheService.SERVICE_NAME, name);
        CacheContext cacheContext = cacheContexts.get(name);
        if (cacheContext != null) {
            cacheContext.resetCacheEntryListenerCount();
            cacheContext.resetInvalidationListenerCount();
        }
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

    @SuppressFBWarnings(value = "JLM_JSR166_UTILCONCURRENT_MONITORENTER", justification =
            "several ops performed on concurrent map, need synchronization for atomicity")
    public void addCacheResource(String name, Closeable resource) {
        Set<Closeable> cacheResources = resources.get(name);
        if (cacheResources == null) {
            synchronized (resources) {
                // In case of creation of resource set for specified cache name, we checks double from resources map.
                // But this happens only once for each cache and this prevents other calls
                // from unnecessary "synchronized" lock on "resources" instance
                // since "cacheResources" will not be for specified cache name.
                cacheResources = resources.get(name);
                if (cacheResources == null) {
                    cacheResources = Collections.newSetFromMap(new ConcurrentHashMap<Closeable, Boolean>());
                    resources.put(name, cacheResources);
                }
            }
        }
        cacheResources.add(resource);
    }

    private void deleteCacheResources(String name) {
        Set<Closeable> cacheResources = resources.remove(name);
        if (cacheResources != null) {
            for (Closeable resource : cacheResources) {
                IOUtil.closeResource(resource);
            }
            cacheResources.clear();
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        PostJoinCacheOperation postJoinCacheOperation = new PostJoinCacheOperation();
        for (Map.Entry<String, CacheConfig> cacheConfigEntry : configs.entrySet()) {
            postJoinCacheOperation.addCacheConfig(cacheConfigEntry.getValue());
        }
        return postJoinCacheOperation;
    }


    protected  void publishCachePartitionLostEvent(String cacheName, int partitionId) {
        final Collection<EventRegistration> registrations = new LinkedList<EventRegistration>();
        for (EventRegistration registration : getRegistrations(cacheName)) {
            if (registration.getFilter() instanceof CachePartitionLostEventFilter) {
                registrations.add(registration);
            }
        }

        if (registrations.isEmpty()) {
            return;
        }
        final Member member = nodeEngine.getLocalMember();
        final CacheEventData eventData = new CachePartitionEventData(cacheName, partitionId, member);
        final EventService eventService = nodeEngine.getEventService();

        eventService.publishEvent(SERVICE_NAME, registrations, eventData, partitionId);

    }

    Collection<EventRegistration> getRegistrations(String cacheName) {
        final EventService eventService = nodeEngine.getEventService();
        return eventService.getRegistrations(SERVICE_NAME, cacheName);
    }

    @Override
    public void onPartitionLost(InternalPartitionLostEvent partitionLostEvent) {
        final int partitionId = partitionLostEvent.getPartitionId();
        for (CacheConfig config : getCacheConfigs()) {
            final String cacheName = config.getName();
            if (config.getBackupCount() <= partitionLostEvent.getLostReplicaIndex()) {
                publishCachePartitionLostEvent(cacheName, partitionId);
            }
        }
    }

    public void cacheEntryListenerRegistered(String name,
                                             CacheEntryListenerConfiguration cacheEntryListenerConfiguration) {
        CacheConfig cacheConfig = getCacheConfig(name);
        if (cacheConfig == null) {
            throw new IllegalStateException("CacheConfig does not exist for cache " + name);
        }
        cacheConfig.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
    }

    public void cacheEntryListenerDeregistered(String name,
                                               CacheEntryListenerConfiguration cacheEntryListenerConfiguration) {
        CacheConfig cacheConfig = getCacheConfig(name);
        if (cacheConfig == null) {
            throw new IllegalStateException("CacheConfig does not exist for cache " + name);
        }
        cacheConfig.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
    }

    /**
     * Gets the name of the quorum associated with specified cache
     *
     * @param cacheName name of the cache
     * @return name of the associated quorum
     *         null if there is no associated quorum
     */
    @Override
    public String getQuorumName(String cacheName) {
        if (configs.get(cacheName) == null) {
            return null;
        }
        return configs.get(cacheName).getQuorumName();
    }

}

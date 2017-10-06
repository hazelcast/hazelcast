/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.journal.CacheEventJournal;
import com.hazelcast.cache.impl.journal.RingbufferCacheEventJournalImpl;
import com.hazelcast.cache.impl.operation.OnJoinCacheOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.Version;

import javax.cache.CacheException;
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

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateCacheConfig;
import static com.hazelcast.internal.cluster.Versions.V3_9;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public abstract class AbstractCacheService implements ICacheService, PreJoinAwareService, PostJoinAwareService,
        PartitionAwareService, QuorumAwareService, SplitBrainHandlerService {

    private static final String SETUP_REF = "setupRef";

    /** Map from full prefixed cache name to {@link CacheConfig} */
    protected final ConcurrentMap<String, CacheConfig> configs = new ConcurrentHashMap<String, CacheConfig>();

    /** Map from full prefixed cache name to {@link CacheContext} */
    protected final ConcurrentMap<String, CacheContext> cacheContexts = new ConcurrentHashMap<String, CacheContext>();

    /** Map from full prefixed cache name to {@link CacheStatisticsImpl} */
    protected final ConcurrentMap<String, CacheStatisticsImpl> statistics = new ConcurrentHashMap<String, CacheStatisticsImpl>();

    /** Map from full prefixed cache name to set of {@link Closeable} resources */
    protected final ConcurrentMap<String, Set<Closeable>> resources = new ConcurrentHashMap<String, Set<Closeable>>();
    protected final ConcurrentMap<String, Closeable> closeableListeners = new ConcurrentHashMap<String, Closeable>();
    protected final ConcurrentMap<String, CacheOperationProvider> operationProviderCache =
            new ConcurrentHashMap<String, CacheOperationProvider>();
    protected final ConstructorFunction<String, CacheContext> cacheContextsConstructorFunction =
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
                    return new CacheStatisticsImpl(
                            Clock.currentTimeMillis(),
                            CacheEntryCountResolver.createEntryCountResolver(getOrCreateCacheContext(name)));
                }
            };
    // mutex factory ensures each Set<Closeable> of cache resources is only constructed and inserted in resources map once
    protected final ContextMutexFactory cacheResourcesMutexFactory = new ContextMutexFactory();
    protected final ConstructorFunction<String, Set<Closeable>> cacheResourcesConstructorFunction =
            new ConstructorFunction<String, Set<Closeable>>() {
                @Override
                public Set<Closeable> createNew(String name) {
                    return Collections.newSetFromMap(new ConcurrentHashMap<Closeable, Boolean>());
                }
            };

    protected NodeEngine nodeEngine;
    protected CachePartitionSegment[] segments;
    protected CacheEventHandler cacheEventHandler;
    protected CacheSplitBrainHandler cacheSplitBrainHandler;
    protected RingbufferCacheEventJournalImpl eventJournal;
    protected ILogger logger;

    @Override
    public final void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.segments = new CachePartitionSegment[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            segments[i] = newPartitionSegment(i);
        }
        this.cacheEventHandler = new CacheEventHandler(nodeEngine);
        this.cacheSplitBrainHandler = new CacheSplitBrainHandler(nodeEngine, configs, segments);
        this.logger = nodeEngine.getLogger(getClass());
        this.eventJournal = new RingbufferCacheEventJournalImpl(nodeEngine);
        postInit(nodeEngine, properties);
    }

    protected void postInit(NodeEngine nodeEngine, Properties properties) {
    }

    protected abstract CachePartitionSegment newPartitionSegment(int partitionId);

    protected abstract ICacheRecordStore createNewRecordStore(String cacheNameWithPrefix, int partitionId);

    @Override
    public void reset() {
        reset(false);
    }

    private void reset(boolean onShutdown) {
        for (String objectName : configs.keySet()) {
            deleteCache(objectName, null, false);
        }
        CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                if (onShutdown) {
                    partitionSegment.shutdown();
                } else {
                    partitionSegment.clear();
                    partitionSegment.init();
                }
            }
        }

        for (String objectName : configs.keySet()) {
            sendInvalidationEvent(objectName, null, SOURCE_NOT_AVAILABLE);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            cacheEventHandler.shutdown();
            reset(true);
        }
    }

    @Override
    public DistributedObject createDistributedObject(String cacheNameWithPrefix) {
        try {
            /*
             * In here, cacheNameWithPrefix is the full cache name.
             * Full cache name contains, Hazelcast prefix, cache name prefix and pure cache name.
             */
            if (cacheNameWithPrefix.equals(SETUP_REF)) {
                // workaround to make clients older than 3.7 to work with 3.7+ servers due to changes in the cache init!
                CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
                cacheSimpleConfig.setName("setupRef");
                CacheConfig cacheConfig = new CacheConfig(cacheSimpleConfig);
                cacheConfig.setManagerPrefix(HazelcastCacheManager.CACHE_MANAGER_PREFIX);
                return new CacheProxy(cacheConfig, nodeEngine, this);
            } else {
                // At first, lookup cache name in the created cache configs.
                CacheConfig cacheConfig = getCacheConfig(cacheNameWithPrefix);
                if (cacheConfig == null) {
                    /*
                     * Prefixed cache name contains cache name prefix and pure cache name, but not Hazelcast prefix (`/hz/`).
                     * Cache name prefix is generated by using specified URI and classloader scopes.
                     * This means, if there is no specified URI and classloader, prefixed cache name is pure cache name.
                     * This means, if there is no specified URI and classloader, prefixed cache name is pure cache name.
                     */
                    // If cache config is not created yet, remove Hazelcast prefix and get prefixed cache name.
                    String cacheName = cacheNameWithPrefix.substring(HazelcastCacheManager.CACHE_MANAGER_PREFIX.length());
                    // Lookup prefixed cache name in the config.
                    cacheConfig = findCacheConfig(cacheName);
                    if (cacheConfig == null) {
                        throw new CacheNotExistsException("Couldn't find cache config with name " + cacheNameWithPrefix);
                    }
                    cacheConfig.setManagerPrefix(HazelcastCacheManager.CACHE_MANAGER_PREFIX);
                }

                validateCacheConfig(cacheConfig);
                putCacheConfigIfAbsent(cacheConfig);

                return new CacheProxy(cacheConfig, nodeEngine, this);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        deleteCache(objectName, null, true);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearCachesHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
        initPartitionReplica(event.getPartitionId());
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearCachesHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
        initPartitionReplica(event.getPartitionId());
    }

    private void clearCachesHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex == -1) {
            clearPartitionReplica(partitionId);
            return;
        }

        CachePartitionSegment segment = segments[partitionId];
        segment.clearHavingLesserBackupCountThan(thresholdReplicaIndex);
    }

    private void initPartitionReplica(int partitionId) {
        segments[partitionId].init();
    }

    private void clearPartitionReplica(int partitionId) {
        segments[partitionId].clear();
    }

    @Override
    public ICacheRecordStore getOrCreateRecordStore(String cacheNameWithPrefix, int partitionId) {
        return segments[partitionId].getOrCreateRecordStore(cacheNameWithPrefix);
    }

    @Override
    public ICacheRecordStore getRecordStore(String cacheNameWithPrefix, int partitionId) {
        return segments[partitionId].getRecordStore(cacheNameWithPrefix);
    }

    @Override
    public CachePartitionSegment getSegment(int partitionId) {
        return segments[partitionId];
    }

    protected void destroySegments(CacheConfig cacheConfig) {
        String name = cacheConfig.getNameWithPrefix();
        for (CachePartitionSegment segment : segments) {
            segment.deleteRecordStore(name, true);
        }
    }

    protected void closeSegments(String name) {
        for (CachePartitionSegment segment : segments) {
            segment.deleteRecordStore(name, false);
        }
    }

    @Override
    public void deleteCache(String cacheNameWithPrefix, String callerUuid, boolean destroy) {
        CacheConfig config = deleteCacheConfig(cacheNameWithPrefix);
        if (config == null) {
            // Cache is already cleaned up
            return;
        }
        if (destroy) {
            cacheEventHandler.destroy(cacheNameWithPrefix, SOURCE_NOT_AVAILABLE);
            destroySegments(config);
        } else {
            closeSegments(cacheNameWithPrefix);
        }
        cacheContexts.remove(cacheNameWithPrefix);
        operationProviderCache.remove(cacheNameWithPrefix);
        deregisterAllListener(cacheNameWithPrefix);
        setStatisticsEnabled(config, cacheNameWithPrefix, false);
        setManagementEnabled(config, cacheNameWithPrefix, false);
        deleteCacheStat(cacheNameWithPrefix);
        deleteCacheResources(cacheNameWithPrefix);
    }

    @Override
    public CacheConfig putCacheConfigIfAbsent(CacheConfig config) {
        CacheConfig localConfig = configs.putIfAbsent(config.getNameWithPrefix(), config);
        if (localConfig == null) {
            if (config.isStatisticsEnabled()) {
                setStatisticsEnabled(config, config.getNameWithPrefix(), true);
            }
            if (config.isManagementEnabled()) {
                setManagementEnabled(config, config.getNameWithPrefix(), true);
            }
        }
        if (localConfig == null) {
            logger.info("Added cache config: " + config);
        }
        return localConfig;
    }

    @Override
    public CacheConfig deleteCacheConfig(String cacheNameWithPrefix) {
        CacheConfig config = configs.remove(cacheNameWithPrefix);
        if (config != null) {
            logger.info("Removed cache config: " + config);
        }
        return config;
    }

    @Override
    public CacheStatisticsImpl createCacheStatIfAbsent(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutIfAbsent(statistics, cacheNameWithPrefix, cacheStatisticsConstructorFunction);
    }

    public CacheContext getCacheContext(String name) {
        return cacheContexts.get(name);
    }

    @Override
    public CacheContext getOrCreateCacheContext(String cacheNameWithPrefix) {
        return ConcurrencyUtil.getOrPutIfAbsent(cacheContexts, cacheNameWithPrefix, cacheContextsConstructorFunction);
    }

    @Override
    public void deleteCacheStat(String cacheNameWithPrefix) {
        statistics.remove(cacheNameWithPrefix);
    }

    @Override
    public void setStatisticsEnabled(CacheConfig cacheConfig, String cacheNameWithPrefix, boolean enabled) {
        cacheConfig = cacheConfig != null ? cacheConfig : configs.get(cacheNameWithPrefix);
        if (cacheConfig != null) {
            String cacheManagerName = cacheConfig.getUriString();
            cacheConfig.setStatisticsEnabled(enabled);
            if (enabled) {
                CacheStatisticsImpl cacheStatistics = createCacheStatIfAbsent(cacheNameWithPrefix);
                CacheStatisticsMXBeanImpl mxBean = new CacheStatisticsMXBeanImpl(cacheStatistics);
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
            String cacheManagerName = cacheConfig.getUriString();
            cacheConfig.setManagementEnabled(enabled);
            if (enabled) {
                CacheMXBeanImpl mxBean = new CacheMXBeanImpl(cacheConfig);
                MXBeanUtil.registerCacheObject(mxBean, cacheManagerName, cacheConfig.getName(), false);
            } else {
                MXBeanUtil.unregisterCacheObject(cacheManagerName, cacheConfig.getName(), false);
                deleteCacheStat(cacheNameWithPrefix);
            }
        }
    }

    @Override
    public CacheConfig getCacheConfig(String cacheNameWithPrefix) {
        return configs.get(cacheNameWithPrefix);
    }

    @Override
    public CacheConfig findCacheConfig(String simpleName) {
        if (simpleName == null) {
            return null;
        }
        CacheSimpleConfig cacheSimpleConfig = nodeEngine.getConfig().findCacheConfig(simpleName);
        if (cacheSimpleConfig == null) {
            return null;
        }
        try {
            // Set name explicitly, because found config might have a wildcard name.
            return new CacheConfig(cacheSimpleConfig).setName(simpleName);
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    @Override
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
        cacheEventHandler.publishEvent(cacheEventContext);
    }

    @Override
    public void publishEvent(String cacheNameWithPrefix, CacheEventSet eventSet, int orderKey) {
        cacheEventHandler.publishEvent(cacheNameWithPrefix, eventSet, orderKey);
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
    public String registerListener(String cacheNameWithPrefix, CacheEventListener listener, boolean isLocal) {
        return registerListenerInternal(cacheNameWithPrefix, listener, null, isLocal);
    }

    @Override
    public String registerListener(String cacheNameWithPrefix, CacheEventListener listener,
                                   EventFilter eventFilter, boolean isLocal) {
        return registerListenerInternal(cacheNameWithPrefix, listener, eventFilter, isLocal);
    }

    protected String registerListenerInternal(String cacheNameWithPrefix, CacheEventListener listener,
                                              EventFilter eventFilter, boolean isLocal) {
        EventService eventService = getNodeEngine().getEventService();
        EventRegistration reg;
        if (isLocal) {
            if (eventFilter == null) {
                reg = eventService.registerLocalListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, listener);
            } else {
                reg = eventService.registerLocalListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix,
                        eventFilter, listener);
            }
        } else {
            if (eventFilter == null) {
                reg = eventService.registerListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix, listener);
            } else {
                reg = eventService.registerListener(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix,
                        eventFilter, listener);
            }
        }

        String id = reg.getId();
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
    public boolean deregisterListener(String cacheNameWithPrefix, String registrationId) {
        EventService eventService = getNodeEngine().getEventService();
        boolean result = eventService.deregisterListener(SERVICE_NAME, cacheNameWithPrefix, registrationId);
        Closeable listener = closeableListeners.remove(registrationId);
        if (listener != null) {
            IOUtil.closeResource(listener);
        }
        return result;
    }

    @Override
    public void deregisterAllListener(String cacheNameWithPrefix) {
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, cacheNameWithPrefix);
        if (registrations != null) {
            for (EventRegistration registration : registrations) {
                Closeable listener = closeableListeners.remove(registration.getId());
                if (listener != null) {
                    IOUtil.closeResource(listener);
                }
            }
        }
        eventService.deregisterAllListeners(AbstractCacheService.SERVICE_NAME, cacheNameWithPrefix);
        CacheContext cacheContext = cacheContexts.get(cacheNameWithPrefix);
        if (cacheContext != null) {
            cacheContext.resetCacheEntryListenerCount();
            cacheContext.resetInvalidationListenerCount();
        }
    }

    @Override
    public CacheStatisticsImpl getStatistics(String cacheNameWithPrefix) {
        return statistics.get(cacheNameWithPrefix);
    }

    @Override
    public CacheOperationProvider getCacheOperationProvider(String cacheNameWithPrefix, InMemoryFormat inMemoryFormat) {
        if (InMemoryFormat.NATIVE.equals(inMemoryFormat)) {
            throw new IllegalArgumentException("Native memory is available only in Hazelcast Enterprise."
                    + "Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }
        CacheOperationProvider cacheOperationProvider = operationProviderCache.get(cacheNameWithPrefix);
        if (cacheOperationProvider != null) {
            return cacheOperationProvider;
        }
        cacheOperationProvider = createOperationProvider(cacheNameWithPrefix, inMemoryFormat);
        CacheOperationProvider current = operationProviderCache.putIfAbsent(cacheNameWithPrefix, cacheOperationProvider);
        return current == null ? cacheOperationProvider : current;
    }

    protected abstract CacheOperationProvider createOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat);

    public void addCacheResource(String cacheNameWithPrefix, Closeable resource) {
        Set<Closeable> cacheResources = ConcurrencyUtil.getOrPutSynchronized(
                resources, cacheNameWithPrefix, cacheResourcesMutexFactory, cacheResourcesConstructorFunction);
        cacheResources.add(resource);
    }

    protected void deleteCacheResources(String name) {
        Set<Closeable> cacheResources;
        ContextMutexFactory.Mutex mutex = cacheResourcesMutexFactory.mutexFor(name);
        try {
            synchronized (mutex) {
                cacheResources = resources.remove(name);
            }
        } finally {
            mutex.close();
        }

        if (cacheResources != null) {
            for (Closeable resource : cacheResources) {
                IOUtil.closeResource(resource);
            }
            cacheResources.clear();
        }
    }

    @Override
    public Operation getPreJoinOperation() {
        Version clusterVersion = nodeEngine.getClusterService().getClusterVersion();
        OnJoinCacheOperation preJoinCacheOperation = null;
        assert !clusterVersion.isUnknown() : "Cluster version should not be unknown";
        if (clusterVersion.isGreaterOrEqual(V3_9)) {
            preJoinCacheOperation = prepareOnJoinCacheOperation(true);
        }
        return preJoinCacheOperation;
    }

    // RU_COMPAT_38 since 3.9, configs are transferred in pre-join operation
    @Override
    public Operation getPostJoinOperation() {
        Version clusterVersion = nodeEngine.getClusterService().getClusterVersion();
        OnJoinCacheOperation postJoinCacheOperation = null;
        assert !clusterVersion.isUnknown() : "Cluster version should not be unknown";
        if (clusterVersion.isLessThan(V3_9)) {
            postJoinCacheOperation = prepareOnJoinCacheOperation(false);
        }
        return postJoinCacheOperation;
    }

    protected void publishCachePartitionLostEvent(String cacheName, int partitionId) {
        Collection<EventRegistration> registrations = new LinkedList<EventRegistration>();
        for (EventRegistration registration : getRegistrations(cacheName)) {
            if (registration.getFilter() instanceof CachePartitionLostEventFilter) {
                registrations.add(registration);
            }
        }

        if (registrations.isEmpty()) {
            return;
        }
        Member member = nodeEngine.getLocalMember();
        CacheEventData eventData = new CachePartitionEventData(cacheName, partitionId, member);
        EventService eventService = nodeEngine.getEventService();

        eventService.publishEvent(SERVICE_NAME, registrations, eventData, partitionId);

    }

    Collection<EventRegistration> getRegistrations(String cacheName) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.getRegistrations(SERVICE_NAME, cacheName);
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        int partitionId = partitionLostEvent.getPartitionId();
        for (CacheConfig config : getCacheConfigs()) {
            final String cacheName = config.getName();
            if (config.getTotalBackupCount() <= partitionLostEvent.getLostReplicaIndex()) {
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
     * null if there is no associated quorum
     */
    @Override
    public String getQuorumName(String cacheName) {
        CacheConfig cacheConfig = configs.get(cacheName);
        if (cacheConfig == null) {
            return null;
        }
        return cacheConfig.getQuorumName();
    }

    /**
     * Registers and {@link com.hazelcast.cache.impl.CacheEventListener} for specified {@code cacheNameWithPrefix}
     *
     * @param cacheNameWithPrefix the full name of the cache (including manager scope prefix)
     *                            that {@link com.hazelcast.cache.impl.CacheEventListener} will be registered for
     * @param listener            the {@link com.hazelcast.cache.impl.CacheEventListener} to be registered
     *                            for specified {@code cacheNameWithPrefix}
     * @param localOnly           true if only events originated from this member wants be listened, false if all
     *                            invalidation events in the cluster wants to be listened
     * @return the ID which is unique for current registration
     */
    @Override
    public String addInvalidationListener(String cacheNameWithPrefix, CacheEventListener listener, boolean localOnly) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        if (localOnly) {
            registration = eventService.registerLocalListener(SERVICE_NAME, cacheNameWithPrefix, listener);
        } else {
            registration = eventService.registerListener(SERVICE_NAME, cacheNameWithPrefix, listener);
        }
        return registration.getId();
    }

    /**
     * Sends an invalidation event for given <code>cacheName</code> with specified <code>key</code>
     * from mentioned source with <code>sourceUuid</code>.
     *
     * @param cacheNameWithPrefix the name of the cache that invalidation event is sent for
     * @param key                 the {@link com.hazelcast.nio.serialization.Data} represents the invalidation event
     * @param sourceUuid          an ID that represents the source for invalidation event
     */
    @Override
    public void sendInvalidationEvent(String cacheNameWithPrefix, Data key, String sourceUuid) {
        cacheEventHandler.sendInvalidationEvent(cacheNameWithPrefix, key, sourceUuid);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return cacheSplitBrainHandler.prepareMergeRunnable();
    }

    public CacheEventHandler getCacheEventHandler() {
        return cacheEventHandler;
    }

    @Override
    public CacheEventJournal getEventJournal() {
        return eventJournal;
    }

    private OnJoinCacheOperation prepareOnJoinCacheOperation(boolean clusterVersionGreaterOrEqualV39) {
        OnJoinCacheOperation preJoinCacheOperation;
        preJoinCacheOperation = new OnJoinCacheOperation();
        for (Map.Entry<String, CacheConfig> cacheConfigEntry : configs.entrySet()) {
            CacheConfig cacheConfig;
            if (clusterVersionGreaterOrEqualV39) {
                cacheConfig = new PreJoinCacheConfig(cacheConfigEntry.getValue());
            } else {
                cacheConfig = cacheConfigEntry.getValue();
            }
            preJoinCacheOperation.addCacheConfig(cacheConfig);
        }
        return preJoinCacheOperation;
    }
}

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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.RingbufferWaitNotifyKey;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.OperationParker;

import static com.hazelcast.cache.CacheEventType.CREATED;
import static com.hazelcast.cache.CacheEventType.EVICTED;
import static com.hazelcast.cache.CacheEventType.EXPIRED;
import static com.hazelcast.cache.CacheEventType.REMOVED;
import static com.hazelcast.cache.CacheEventType.UPDATED;


/**
 * The cache event journal implementation based on the {@link com.hazelcast.ringbuffer.Ringbuffer}.
 * It will add all journal events into a {@link RingbufferContainer} with the provided namespace
 * and partition ID and allows checking if the cache has a configured event journal.
 * Adapts the {@link EventJournalConfig} to the {@link RingbufferConfig} when creating the ringbuffer.
 */
public class RingbufferCacheEventJournalImpl implements CacheEventJournal {
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    public RingbufferCacheEventJournalImpl(NodeEngine engine) {
        this.nodeEngine = (NodeEngineImpl) engine;
        this.logger = this.nodeEngine.getLogger(RingbufferCacheEventJournalImpl.class);
    }

    @Override
    public void writeUpdateEvent(ObjectNamespace namespace, int partitionId, Data key, Object oldValue, Object newValue) {
        addToEventRingbuffer(namespace, partitionId, UPDATED, key, oldValue, newValue);
    }

    @Override
    public void writeCreatedEvent(ObjectNamespace namespace, int partitionId, Data key, Object value) {
        addToEventRingbuffer(namespace, partitionId, CREATED, key, null, value);
    }

    @Override
    public void writeRemoveEvent(ObjectNamespace namespace, int partitionId, Data key, Object value) {
        addToEventRingbuffer(namespace, partitionId, REMOVED, key, value, null);
    }

    @Override
    public void writeEvictEvent(ObjectNamespace namespace, int partitionId, Data key, Object value) {
        addToEventRingbuffer(namespace, partitionId, EVICTED, key, value, null);
    }

    @Override
    public void writeExpiredEvent(ObjectNamespace namespace, int partitionId, Data key, Object value) {
        addToEventRingbuffer(namespace, partitionId, EXPIRED, key, value, null);
    }

    @Override
    public long newestSequence(ObjectNamespace namespace, int partitionId) {
        return getRingbufferOrFail(namespace, partitionId).tailSequence();
    }

    @Override
    public long oldestSequence(ObjectNamespace namespace, int partitionId) {
        return getRingbufferOrFail(namespace, partitionId).headSequence();
    }

    @Override
    public void destroy(ObjectNamespace namespace, int partitionId) {
        final RingbufferService service;
        try {
            service = getRingbufferService();
        } catch (Exception e) {
            logger.fine("Could not retrieve ringbuffer service to destroy event journal " + namespace, e);
            return;
        }
        service.destroyContainer(partitionId, namespace);
    }

    @Override
    public void isAvailableOrNextSequence(ObjectNamespace namespace, int partitionId, long sequence) {
        getRingbufferOrFail(namespace, partitionId).checkBlockableReadSequence(sequence);
    }

    @Override
    public boolean isNextAvailableSequence(ObjectNamespace namespace, int partitionId, long sequence) {
        return getRingbufferOrFail(namespace, partitionId).shouldWait(sequence);
    }

    @Override
    public WaitNotifyKey getWaitNotifyKey(ObjectNamespace namespace, int partitionId) {
        return new RingbufferWaitNotifyKey(namespace);
    }

    @Override
    public <T> long readMany(ObjectNamespace namespace, int partitionId, long beginSequence,
                             ReadResultSetImpl<InternalEventJournalCacheEvent, T> resultSet) {
        return getRingbufferOrFail(namespace, partitionId).readMany(beginSequence, resultSet);
    }

    @Override
    public void cleanup(ObjectNamespace namespace, int partitionId) {
        getRingbufferOrFail(namespace, partitionId).cleanup();
    }

    /**
     * {@inheritDoc}
     * NOTE: The cache config should have already been created in the cache service before
     * invoking this method.
     *
     * @param namespace the cache namespace, containing the full prefixed cache name
     * @return {@code true} if the object has a configured and enabled event journal, {@code false} otherwise
     */
    @Override
    public boolean hasEventJournal(ObjectNamespace namespace) {
        return getEventJournalConfig(namespace) != null;
    }

    @Override
    public EventJournalConfig getEventJournalConfig(ObjectNamespace namespace) {
        final String name = namespace.getObjectName();
        final CacheConfig cacheConfig = getCacheService().getCacheConfig(name);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache " + name + " is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        final String cacheSimpleName = cacheConfig.getName();
        final EventJournalConfig config = nodeEngine.getConfig().findCacheEventJournalConfig(cacheSimpleName);
        if (config == null || !config.isEnabled()) {
            return null;
        }
        return config;
    }

    @Override
    public RingbufferConfig toRingbufferConfig(EventJournalConfig config) {
        return new RingbufferConfig()
                .setAsyncBackupCount(0)
                .setBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCapacity(config.getCapacity())
                .setTimeToLiveSeconds(config.getTimeToLiveSeconds());
    }

    private void addToEventRingbuffer(ObjectNamespace namespace, int partitionId, CacheEventType eventType,
                                      Data key, Object oldValue, Object newValue) {
        final RingbufferContainer<InternalEventJournalCacheEvent> eventContainer = getRingbufferOrNull(namespace, partitionId);
        if (eventContainer == null) {
            return;
        }
        final InternalEventJournalCacheEvent event
                = new InternalEventJournalCacheEvent(toData(key), toData(newValue), toData(oldValue), eventType.getType());
        eventContainer.add(event);
        getOperationParker().unpark(eventContainer);
    }

    protected Data toData(Object val) {
        return getSerializationService().toData(val, DataType.HEAP);
    }

    /**
     * Gets or creates a ringbuffer for an event journal or throws an exception if no
     * event journal is configured. The caller should call
     * {@link #hasEventJournal(ObjectNamespace)} before invoking this method to avoid getting
     * the exception. This method can be used to get the ringbuffer on which future events
     * will be added once the cache has been created.
     * <p>
     * NOTE: The cache config should have already been created in the cache service before
     * invoking this method.
     *
     * @param namespace   the cache namespace, containing the full prefixed cache name
     * @param partitionId the cache partition ID
     * @return the cache partition event journal
     * @throws IllegalStateException if there is no event journal configured for this cache
     */
    private RingbufferContainer<InternalEventJournalCacheEvent> getRingbufferOrFail(ObjectNamespace namespace, int partitionId) {
        final RingbufferService ringbufferService = getRingbufferService();
        final RingbufferContainer<InternalEventJournalCacheEvent> container =
                ringbufferService.getContainerOrNull(partitionId, namespace);
        if (container != null) {
            return container;
        }

        final EventJournalConfig config = getEventJournalConfig(namespace);
        if (config == null) {
            throw new IllegalStateException("There is no event journal configured for cache with name: "
                    + namespace.getObjectName());
        }
        return getOrCreateRingbufferContainer(namespace, partitionId, config);
    }

    /**
     * Gets or creates a ringbuffer for an event journal or returns {@link null} if no
     * event journal is configured. The cache record store should have been already created
     * at the point when this method is invoked.
     * This method can be used to get the ringbuffer when we already know that the cache record
     * store has been created.
     * <p>
     * NOTE: The cache config should have already been created in the cache service before
     * invoking this method.
     *
     * @param namespace   the cache namespace, containing the full prefixed cache name
     * @param partitionId the cache partition ID
     * @return the cache partition event journal or {@code null} if no journal is configured for this cache
     * @throws CacheNotExistsException if the cache hasn't been already created
     */
    private RingbufferContainer<InternalEventJournalCacheEvent> getRingbufferOrNull(ObjectNamespace namespace, int partitionId) {
        final RingbufferService ringbufferService = getRingbufferService();
        final RingbufferContainer<InternalEventJournalCacheEvent> container =
                ringbufferService.getContainerOrNull(partitionId, namespace);
        if (container != null) {
            return container;
        }

        final EventJournalConfig config = getEventJournalConfig(namespace);
        return config != null ? getOrCreateRingbufferContainer(namespace, partitionId, config) : null;
    }

    private RingbufferContainer<InternalEventJournalCacheEvent> getOrCreateRingbufferContainer(ObjectNamespace namespace,
                                                                                               int partitionId,
                                                                                               EventJournalConfig config) {
        final RingbufferConfig ringbufferConfig = toRingbufferConfig(config);
        return getRingbufferService().getOrCreateContainer(partitionId, namespace, ringbufferConfig);
    }

    private RingbufferService getRingbufferService() {
        return nodeEngine.getService(RingbufferService.SERVICE_NAME);
    }

    private OperationParker getOperationParker() {
        return nodeEngine.getOperationParker();
    }

    private InternalSerializationService getSerializationService() {
        return (InternalSerializationService) nodeEngine.getSerializationService();
    }

    private CacheService getCacheService() {
        return nodeEngine.getService(CacheService.SERVICE_NAME);
    }
}

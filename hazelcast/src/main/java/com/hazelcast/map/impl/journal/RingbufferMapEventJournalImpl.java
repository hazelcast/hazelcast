/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.journal;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.RingbufferWaitNotifyKey;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.OperationParker;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.LOADED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;

/**
 * The map event journal implementation based on the {@link com.hazelcast.ringbuffer.Ringbuffer}.
 * It will add all journal events into a {@link RingbufferContainer} with the provided namespace
 * and partition ID and allows checking if the map has a configured event journal.
 * Adapts the {@link EventJournalConfig} to the {@link RingbufferConfig} when creating the ringbuffer.
 */
public class RingbufferMapEventJournalImpl implements MapEventJournal {

    private final NodeEngineImpl nodeEngine;
    private final MapServiceContext mapServiceContext;
    private final ILogger logger;

    public RingbufferMapEventJournalImpl(NodeEngine engine, MapServiceContext mapServiceContext) {
        this.nodeEngine = (NodeEngineImpl) engine;
        this.mapServiceContext = mapServiceContext;
        this.logger = this.nodeEngine.getLogger(RingbufferMapEventJournalImpl.class);
    }

    @Override
    public void writeUpdateEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                 Data key, Object oldValue, Object newValue) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, UPDATED, key, oldValue, newValue);
    }

    @Override
    public void writeAddEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                              Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, ADDED, key, null, value);
    }

    @Override
    public void writeRemoveEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                 Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, REMOVED, key, value, null);
    }

    @Override
    public void writeEvictEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                Data key, Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, EVICTED, key, value, null);
    }

    @Override
    public void writeLoadEvent(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId, Data key,
                               Object value) {
        addToEventRingbuffer(journalConfig, namespace, partitionId, LOADED, key, null, value);
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
    public boolean isPersistenceEnabled(ObjectNamespace namespace, int partitionId) {
        return getRingbufferOrFail(namespace, partitionId).getStore().isEnabled();
    }

    @Override
    public void destroy(ObjectNamespace namespace, int partitionId) {
        RingbufferService service;
        try {
            service = getRingbufferService();
        } catch (Exception e) {
            if (nodeEngine.isRunning()) {
                logger.fine("Could not retrieve ringbuffer service to destroy event journal " + namespace, e);
            }
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
        return new RingbufferWaitNotifyKey(namespace, partitionId);
    }

    @Override
    public <T> long readMany(ObjectNamespace namespace, int partitionId, long beginSequence,
                             ReadResultSetImpl<InternalEventJournalMapEvent, T> resultSet) {
        return getRingbufferOrFail(namespace, partitionId).readMany(beginSequence, resultSet);
    }

    @Override
    public void cleanup(ObjectNamespace namespace, int partitionId) {
        getRingbufferOrFail(namespace, partitionId).cleanup();
    }

    @Override
    public boolean hasEventJournal(ObjectNamespace namespace) {
        EventJournalConfig config = getEventJournalConfig(namespace);
        return config != null && config.isEnabled();
    }

    @Override
    public EventJournalConfig getEventJournalConfig(ObjectNamespace namespace) {
        return nodeEngine.getConfig()
                         .findMapConfig(namespace.getObjectName())
                         .getEventJournalConfig();
    }

    @Override
    public RingbufferConfig toRingbufferConfig(EventJournalConfig config, ObjectNamespace namespace) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(namespace.getObjectName());
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return new RingbufferConfig()
                .setAsyncBackupCount(mapContainer.getAsyncBackupCount())
                .setBackupCount(mapContainer.getBackupCount())
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCapacity(config.getCapacity() / partitionCount)
                .setTimeToLiveSeconds(config.getTimeToLiveSeconds());
    }

    private void addToEventRingbuffer(EventJournalConfig journalConfig, ObjectNamespace namespace, int partitionId,
                                      EntryEventType eventType, Data key, Object oldValue, Object newValue) {
        if (journalConfig == null || !journalConfig.isEnabled()) {
            return;
        }
        RingbufferContainer<InternalEventJournalMapEvent, Object> eventContainer = getRingbufferOrNull(namespace, partitionId);
        if (eventContainer == null) {
            return;
        }
        InternalEventJournalMapEvent event
                = new InternalEventJournalMapEvent(toData(key), toData(newValue), toData(oldValue), eventType.getType());
        eventContainer.add(event);
        getOperationParker().unpark(eventContainer);
    }

    private Data toData(Object val) {
        return getSerializationService().toData(val, DataType.HEAP);
    }

    private RingbufferContainer<InternalEventJournalMapEvent, Object> getRingbufferOrFail(ObjectNamespace namespace,
                                                                                          int partitionId) {
        RingbufferContainer<InternalEventJournalMapEvent, Object> ringbuffer = getRingbufferOrNull(namespace, partitionId);
        if (ringbuffer == null) {
            throw new IllegalStateException("There is no event journal configured for map with name: "
                    + namespace.getObjectName());
        }
        return ringbuffer;
    }

    private RingbufferContainer<InternalEventJournalMapEvent, Object> getRingbufferOrNull(ObjectNamespace namespace,
                                                                                          int partitionId) {
        RingbufferService service = getRingbufferService();
        RingbufferConfig ringbufferConfig;
        RingbufferContainer<InternalEventJournalMapEvent, Object> container
                = service.getContainerOrNull(partitionId, namespace);
        if (container != null) {
            return container;
        }

        EventJournalConfig config = getEventJournalConfig(namespace);
        if (config == null || !config.isEnabled()) {
            return null;
        }
        ringbufferConfig = toRingbufferConfig(config, namespace);
        return service.getOrCreateContainer(partitionId, namespace, ringbufferConfig);
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
}

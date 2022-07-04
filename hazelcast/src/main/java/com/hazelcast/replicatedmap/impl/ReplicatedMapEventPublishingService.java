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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.event.MapEventData;
import com.hazelcast.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Dispatches published events on replicated map to corresponding listeners.
 */
public class ReplicatedMapEventPublishingService
        implements EventPublishingService {

    private final HashMap<String, Boolean> statisticsMap = new HashMap<>();

    private final ReplicatedMapService replicatedMapService;
    private final NodeEngine nodeEngine;
    private final Config config;
    private final EventService eventService;
    private final InternalSerializationService serializationService;
    private final Extractors extractors;

    public ReplicatedMapEventPublishingService(ReplicatedMapService replicatedMapService) {
        this.replicatedMapService = replicatedMapService;
        this.nodeEngine = replicatedMapService.getNodeEngine();
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.extractors = Extractors.newBuilder(this.serializationService).build();
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        if ((event instanceof EntryEventData)) {
            EntryEventData entryEventData = (EntryEventData) event;
            Member member = getMember(entryEventData);
            EntryEvent entryEvent = createDataAwareEntryEvent(entryEventData, member);
            EntryListener entryListener = (EntryListener) listener;
            switch (entryEvent.getEventType()) {
                case ADDED:
                    entryListener.entryAdded(entryEvent);
                    break;
                case EVICTED:
                    entryListener.entryEvicted(entryEvent);
                    break;
                case UPDATED:
                    entryListener.entryUpdated(entryEvent);
                    break;
                case REMOVED:
                    entryListener.entryRemoved(entryEvent);
                    break;
                default:
                    throw new IllegalArgumentException("event type " + entryEvent.getEventType() + " not supported");
            }

            String mapName = ((EntryEventData) event).getMapName();
            Boolean statisticsEnabled = statisticsMap.get(mapName);
            if (statisticsEnabled == null) {
                ReplicatedMapConfig mapConfig = config.findReplicatedMapConfig(mapName);
                statisticsEnabled = mapConfig.isStatisticsEnabled();
                statisticsMap.put(mapName, statisticsEnabled);
            }
            if (statisticsEnabled) {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(entryEventData.getDataKey());
                ReplicatedRecordStore recordStore = replicatedMapService.getPartitionContainer(partitionId)
                                                                        .getRecordStore(mapName);
                if (recordStore instanceof AbstractReplicatedRecordStore) {
                    LocalReplicatedMapStatsImpl stats = ((AbstractReplicatedRecordStore) recordStore).getStats();
                    stats.incrementReceivedEvents();
                }
            }
        } else if (event instanceof MapEventData) {
            MapEventData mapEventData = (MapEventData) event;
            Member member = getMember(mapEventData);
            MapEvent mapEvent = new MapEvent(mapEventData.getMapName(), member,
                    mapEventData.getEventType(), mapEventData.getNumberOfEntries());
            EntryListener entryListener = (EntryListener) listener;
            EntryEventType type = EntryEventType.getByType(mapEventData.getEventType());
            if (type == EntryEventType.CLEAR_ALL) {
                entryListener.mapCleared(mapEvent);
            } else {
                throw new IllegalArgumentException("Unsupported EntryEventType: " + type);
            }
        }
    }

    public @Nonnull
    UUID addLocalEventListener(EventListener entryListener, EventFilter eventFilter, String mapName) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, eventFilter,
                entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(@Nonnull String mapName, @Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    public Future<Boolean> removeEventListenerAsync(@Nonnull String mapName, @Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        return eventService.deregisterListenerAsync(SERVICE_NAME, mapName, registrationId);
    }

    public void fireMapClearedEvent(int deletedEntrySize, String name) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (registrations.isEmpty()) {
            return;
        }
        MapEventData mapEventData = new MapEventData(name, name, nodeEngine.getThisAddress(),
                EntryEventType.CLEAR_ALL.getType(), deletedEntrySize);
        eventService.publishEvent(SERVICE_NAME, registrations, mapEventData, name.hashCode());
    }

    private Member getMember(EventData eventData) {
        Member member = replicatedMapService.getNodeEngine().getClusterService().getMember(eventData.getCaller());
        if (member == null) {
            member = new MemberImpl.Builder(eventData.getCaller())
                    .version(nodeEngine.getVersion())
                    .build();
        }
        return member;
    }

    private DataAwareEntryEvent createDataAwareEntryEvent(EntryEventData entryEventData, Member member) {
        return new DataAwareEntryEvent(member, entryEventData.getEventType(), entryEventData.getMapName(),
                entryEventData.getDataKey(), entryEventData.getDataNewValue(), entryEventData.getDataOldValue(),
                entryEventData.getDataMergingValue(), nodeEngine.getSerializationService());
    }

    public void fireEntryListenerEvent(Data key, Data oldValue, Data value, String name, Address caller) {
        EntryEventType eventType = value == null ? REMOVED : oldValue == null ? ADDED : UPDATED;
        fireEntryListenerEvent(key, oldValue, value, eventType, name, caller);
    }

    public void fireEntryListenerEvent(Data key, Data oldValue, Data value, EntryEventType eventType, String name,
                                       Address caller) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (registrations.isEmpty()) {
            return;
        }
        EntryEventData eventData = new EntryEventData(name, name, caller, key, value, oldValue, eventType.getType());
        for (EventRegistration registration : registrations) {
            if (!shouldPublish(key, oldValue, value, eventType, registration.getFilter())) {
                continue;
            }
            eventService.publishEvent(SERVICE_NAME, registration, eventData, key.hashCode());
        }
    }

    private boolean shouldPublish(Data key, Data oldValue, Data value, EntryEventType eventType, EventFilter filter) {
        QueryEntry queryEntry = null;
        if (filter instanceof ReplicatedQueryEventFilter) {
            Data testValue;
            if (eventType == REMOVED) {
                testValue = oldValue;
            } else {
                testValue = value;
            }
            queryEntry = new QueryEntry(serializationService, key, testValue, extractors);
        }
        return filter == null || filter.eval(queryEntry != null ? queryEntry : key);
    }
}

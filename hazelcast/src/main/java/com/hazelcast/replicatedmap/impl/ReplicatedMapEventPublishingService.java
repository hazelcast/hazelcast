/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.event.MapEventData;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Dispatches published events on replicated map to corresponding listeners.
 */
public class ReplicatedMapEventPublishingService implements EventPublishingService {

    private final HashMap<String, Boolean> statisticsMap = new HashMap<String, Boolean>();

    private final ReplicatedMapService replicatedMapService;
    private final NodeEngine nodeEngine;
    private final Config config;
    private final EventService eventService;

    public ReplicatedMapEventPublishingService(ReplicatedMapService replicatedMapService) {
        this.replicatedMapService = replicatedMapService;
        this.nodeEngine = replicatedMapService.getNodeEngine();
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
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
            switch (type) {
                case CLEAR_ALL:
                    entryListener.mapCleared(mapEvent);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported EntryEventType: " + type);
            }
        }
    }

    public String addEventListener(EventListener entryListener, EventFilter eventFilter, String mapName) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, eventFilter,
                entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(String mapName, String registrationId) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        if (registrationId == null) {
            throw new IllegalArgumentException("registrationId cannot be null");
        }
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
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
            member = new MemberImpl(eventData.getCaller(), nodeEngine.getVersion(), false);
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
            InternalSerializationService serializationService
                    = (InternalSerializationService) nodeEngine.getSerializationService();
            queryEntry = new QueryEntry(serializationService, key, testValue, null);
        }
        return filter == null || filter.eval(queryEntry != null ? queryEntry : key);
    }
}

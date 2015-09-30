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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.event.MapEventData;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import java.util.Collection;

/**
 * Dispatches published events on replicated map to corresponding listeners.
 */
public class ReplicatedMapEventPublishingService implements EventPublishingService {
    private final ReplicatedMapService replicatedMapService;
    private final NodeEngine nodeEngine;

    public ReplicatedMapEventPublishingService(ReplicatedMapService replicatedMapService) {
        this.replicatedMapService = replicatedMapService;
        this.nodeEngine = replicatedMapService.getNodeEngine();
    }

    public void fireMapClearedEvent(int deletedEntrySize, String name) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(
                ReplicatedMapService.SERVICE_NAME, name);
        if (registrations.isEmpty()) {
            return;
        }
        MapEventData mapEventData = new MapEventData(name, name, nodeEngine.getThisAddress(),
                EntryEventType.CLEAR_ALL.getType(), deletedEntrySize);
        for (EventRegistration registration : registrations) {
            eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registration, mapEventData, name.hashCode());
        }
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
                // TODO handle evictAll and clearAll event
                default:
                    throw new IllegalArgumentException("event type " + entryEvent.getEventType() + " not supported");
            }
            String mapName = ((EntryEventData) event).getMapName();
            if (replicatedMapService.getConfig().findReplicatedMapConfig(mapName).isStatisticsEnabled()) {
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
                    throw new IllegalArgumentException("event type " + type + " not supported");
            }
        }
    }

    private Member getMember(EventData eventData) {
        Member member = replicatedMapService.getNodeEngine().getClusterService().getMember(eventData.getCaller());
        if (member == null) {
            member = new MemberImpl(eventData.getCaller(), false);
        }
        return member;
    }

    private DataAwareEntryEvent createDataAwareEntryEvent(EntryEventData entryEventData, Member member) {
        return new DataAwareEntryEvent(member, entryEventData.getEventType(), entryEventData.getMapName(),
                entryEventData.getDataKey(), entryEventData.getDataNewValue(), entryEventData.getDataOldValue(),
                entryEventData.getDataMergingValue(), nodeEngine.getSerializationService());
    }

}

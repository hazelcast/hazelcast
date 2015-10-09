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

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import java.util.EventListener;
import java.util.HashMap;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Dispatches published events on replicated map to corresponding listeners.
 */
public class ReplicatedMapEventPublishingService implements EventPublishingService {
    private final ReplicatedMapService replicatedMapService;
    private final NodeEngine nodeEngine;
    private final Config config;
    private final EventService eventService;
    private final HashMap<String, Boolean> statisticsMap = new HashMap<String, Boolean>();

    public ReplicatedMapEventPublishingService(ReplicatedMapService replicatedMapService) {
        this.replicatedMapService = replicatedMapService;
        this.nodeEngine = replicatedMapService.getNodeEngine();
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        if (nodeEngine.getConfig().isLiteMember()) {
            final Class<?> eventClazz = event.getClass();
            nodeEngine.getLogger(ReplicatedMapService.class).warning("Replicated map event ignored by lite member " + eventClazz);
            return;
        }

        if (!(event instanceof EntryEventData)) {
            return;
        }
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
    }

    public String addEventListener(EventListener entryListener, EventFilter eventFilter, String mapName) {
        if (config.isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, eventFilter,
                entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(String mapName, String registrationId) {
        if (config.isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        if (registrationId == null) {
            throw new IllegalArgumentException("registrationId cannot be null");
        }
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
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

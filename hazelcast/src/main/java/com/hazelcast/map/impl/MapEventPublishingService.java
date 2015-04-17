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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.NodeEngine;

/**
 * Contains map service event publishing service functionality.
 *
 * @see com.hazelcast.spi.EventPublishingService
 */
class MapEventPublishingService implements EventPublishingService<EventData, ListenerAdapter> {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    protected MapEventPublishingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public void dispatchEvent(EventData eventData, ListenerAdapter listener) {
        if (eventData instanceof EntryEventData) {
            dispatchEntryEventData((EntryEventData) eventData, listener);
            return;
        }
        if (eventData instanceof MapEventData) {
            dispatchMapEventData((MapEventData) eventData, listener);
            return;
        }

        if (eventData instanceof MapPartitionEventData) {
            dispatchMapPartitionLostEventData((MapPartitionEventData) eventData, listener);
            return;
        }

        throw new IllegalArgumentException("Unknown map event data");
    }

    private void incrementEventStats(IMapEvent event) {
        final String mapName = event.getName();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapServiceContext.getLocalMapStatsProvider()
                    .getLocalMapStatsImpl(mapName).incrementReceivedEvents();
        }
    }

    private void dispatchMapEventData(MapEventData mapEventData, ListenerAdapter listener) {
        Member member = getMember(mapEventData);
        MapEvent event = createMapEvent(mapEventData, member);
        callListener(listener, event);
    }


    private void dispatchMapPartitionLostEventData(MapPartitionEventData mapPartitionEventData, ListenerAdapter listener) {
        Member member = getMember(mapPartitionEventData);
        MapPartitionLostEvent event = createMapPartitionLostEventData(mapPartitionEventData, member);
        callListener(listener, event);
    }

    private MapPartitionLostEvent createMapPartitionLostEventData(MapPartitionEventData mapPartitionEventData, Member member) {
        return new MapPartitionLostEvent(mapPartitionEventData.getMapName(), member,
                    mapPartitionEventData.getEventType(), mapPartitionEventData.getPartitionId());
    }

    private void callListener(ListenerAdapter listener, IMapEvent event) {
        listener.onEvent(event);
        incrementEventStats(event);
    }

    private MapEvent createMapEvent(MapEventData mapEventData, Member member) {
        return new MapEvent(mapEventData.getMapName(), member,
                mapEventData.getEventType(), mapEventData.getNumberOfEntries());
    }

    private void dispatchEntryEventData(EntryEventData entryEventData, ListenerAdapter listener) {
        Member member = getMember(entryEventData);
        EntryEvent event = createDataAwareEntryEvent(entryEventData, member);
        callListener(listener, event);
    }

    private Member getMember(EventData eventData) {
        Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
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

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

package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.Invalidation;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.NodeEngine;

/**
 * Contains map service event publishing service functionality.
 *
 * @see com.hazelcast.spi.EventPublishingService
 */
public class MapEventPublishingService implements EventPublishingService<Object, ListenerAdapter> {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public MapEventPublishingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public void dispatchEvent(Object eventData, ListenerAdapter listener) {
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

        if (eventData instanceof Invalidation) {
            listener.onEvent(eventData);
            incrementEventStats(((Invalidation) eventData));
            return;
        }

        throw new IllegalArgumentException("Unknown event data [" + eventData + ']');
    }

    private void incrementEventStats(Invalidation data) {
        String mapName = data.getName();
        incrementEventStatsInternal(mapName);
    }

    private void incrementEventStats(IMapEvent event) {
        String mapName = event.getName();
        incrementEventStatsInternal(mapName);
    }

    private void incrementEventStatsInternal(String mapName) {
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


    private void dispatchMapPartitionLostEventData(MapPartitionEventData eventData, ListenerAdapter listener) {
        Member member = getMember(eventData);
        MapPartitionLostEvent event = createMapPartitionLostEventData(eventData, member);
        callListener(listener, event);
    }

    private MapPartitionLostEvent createMapPartitionLostEventData(MapPartitionEventData eventData, Member member) {
        return new MapPartitionLostEvent(eventData.getMapName(), member, eventData.getEventType(), eventData.getPartitionId());
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

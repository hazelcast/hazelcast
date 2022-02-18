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

package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.MapEvent;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.event.BatchEventData;
import com.hazelcast.map.impl.querycache.event.BatchIMapEvent;
import com.hazelcast.map.impl.querycache.event.LocalCacheWideEventData;
import com.hazelcast.map.impl.querycache.event.LocalEntryEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.SingleIMapEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.serialization.SerializationService;

import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.createIMapEvent;

/**
 * Contains map service event publishing service functionality.
 *
 * @see EventPublishingService
 */
public class MapEventPublishingService implements EventPublishingService<Object, ListenerAdapter> {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final SerializationService serializationService;

    public MapEventPublishingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public void dispatchEvent(Object eventData, ListenerAdapter listener) {
        if (eventData instanceof QueryCacheEventData) {
            dispatchQueryCacheEventData((QueryCacheEventData) eventData, listener);
            return;
        }

        if (eventData instanceof BatchEventData) {
            dispatchBatchEventData((BatchEventData) eventData, listener);
            return;
        }

        if (eventData instanceof LocalEntryEventData) {
            dispatchLocalEventData(((LocalEntryEventData) eventData), listener);
            return;
        }

        if (eventData instanceof LocalCacheWideEventData) {
            dispatchLocalEventData(((LocalCacheWideEventData) eventData), listener);
            return;
        }

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

    private void dispatchQueryCacheEventData(QueryCacheEventData eventData, ListenerAdapter listener) {
        SingleIMapEvent mapEvent = createSingleIMapEvent(eventData);
        listener.onEvent(mapEvent);
    }

    private SingleIMapEvent createSingleIMapEvent(QueryCacheEventData eventData) {
        return new SingleIMapEvent(eventData);
    }

    /**
     * Dispatches an event-data to {@link com.hazelcast.map.QueryCache QueryCache} listeners on this local
     * node.
     *
     * @param eventData {@link EventData} to be dispatched
     * @param listener  the listener which the event will be dispatched from
     */
    private void dispatchLocalEventData(EventData eventData, ListenerAdapter listener) {
        IMapEvent event = createIMapEvent(eventData, null, nodeEngine.getLocalMember(), serializationService);
        listener.onEvent(event);
    }

    private void dispatchBatchEventData(BatchEventData batchEventData, ListenerAdapter listener) {
        BatchIMapEvent mapEvent = createBatchEvent(batchEventData);
        listener.onEvent(mapEvent);
    }

    private BatchIMapEvent createBatchEvent(BatchEventData batchEventData) {
        return new BatchIMapEvent(batchEventData);
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
}

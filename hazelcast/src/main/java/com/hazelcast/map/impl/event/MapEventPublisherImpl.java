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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapPartitionLostEventFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SyntheticEventFilter;
import com.hazelcast.map.impl.query.QueryEventFilter;
import com.hazelcast.map.impl.wan.MapReplicationRemove;
import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.isEmpty;

public class MapEventPublisherImpl implements MapEventPublisher {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final SerializationService serializationService;
    protected final EventService eventService;

    public MapEventPublisherImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.eventService = nodeEngine.getEventService();
    }

    @Override
    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapReplicationUpdate replicationEvent
                = new MapReplicationUpdate(mapName, mapContainer.getWanMergePolicy(), entryView);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(SERVICE_NAME, replicationEvent);
    }

    @Override
    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapReplicationRemove event = new MapReplicationRemove(mapName, key, removeTime);
        publishWanReplicationEventInternal(mapName, event);
    }

    @Override
    public void publishMapEvent(Address caller, String mapName, EntryEventType eventType,
                                int numberOfEntriesAffected) {
        Collection<EventRegistration> mapsListenerRegistrations = getRegistrations(mapName);
        if (isEmpty(mapsListenerRegistrations)) {
            return;
        }

        Collection<EventRegistration> registrations = null;
        for (EventRegistration registration : mapsListenerRegistrations) {
            EventFilter filter = registration.getFilter();

            if (filter instanceof EventListenerFilter) {
                if (!filter.eval(eventType.getType())) {
                    continue;
                }
            }

            if (!(filter instanceof MapPartitionLostEventFilter)) {
                if (registrations == null) {
                    registrations = new ArrayList<EventRegistration>();
                }
                registrations.add(registration);
            }
        }

        if (isEmpty(registrations)) {
            return;
        }

        String source = getThisNodesAddress();
        MapEventData mapEventData = new MapEventData(source, mapName, caller, eventType.getType(), numberOfEntriesAffected);
        publishEventInternal(registrations, mapEventData, mapName.hashCode());
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType,
                             Data dataKey, Data dataOldValue, Data dataValue) {
        publishEvent(caller, mapName, eventType, false, dataKey, dataOldValue, dataValue);
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean syntheticEvent,
                             Data dataKey, Data dataOldValue, Data dataValue) {
        publishEvent(caller, mapName, eventType, syntheticEvent, dataKey, dataOldValue, dataValue, null);
    }

    @Override
    public void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean syntheticEvent,
                             Data dataKey, Data dataOldValue, Data dataValue, Data dataMergingValue) {
        Collection<EventRegistration> registrations = getRegistrations(mapName);
        if (isEmpty(registrations)) {
            return;
        }

        List<EventRegistration> includeValueRegistrations = null;
        List<EventRegistration> nullValueRegistrations = null;

        for (EventRegistration registration : registrations) {
            EventFilter filter = registration.getFilter();

            if (!doFilter(filter, syntheticEvent, dataKey, dataOldValue, dataValue, eventType)) {
                continue;
            }

            if (isIncludeValue(filter)) {
                includeValueRegistrations = getOrCreateList(includeValueRegistrations);
                includeValueRegistrations.add(registration);
            } else {
                nullValueRegistrations = getOrCreateList(nullValueRegistrations);
                nullValueRegistrations.add(registration);
            }
        }

        if (!isEmpty(includeValueRegistrations)) {
            EntryEventData eventData = createEntryEventData(mapName, caller, dataKey,
                    dataValue, dataOldValue, dataMergingValue, eventType.getType());
            int orderKey = pickOrderKey(dataKey);
            publishEventInternal(includeValueRegistrations, eventData, orderKey);
        }

        if (!isEmpty(nullValueRegistrations)) {
            EntryEventData eventData = createEntryEventData(mapName, caller, dataKey,
                    null, null, null, eventType.getType());
            int orderKey = pickOrderKey(dataKey);
            publishEventInternal(nullValueRegistrations, eventData, orderKey);
        }
    }

    protected List<EventRegistration> getOrCreateList(List<EventRegistration> registrations) {
        if (registrations == null) {
            registrations = new ArrayList<EventRegistration>();
        }
        return registrations;
    }

    //CHECKSTYLE:OFF
    protected boolean doFilter(EventFilter filter, boolean syntheticEvent, Data dataKey,
                               Data dataOldValue, Data dataValue, EntryEventType eventType) {

        if (filter instanceof MapPartitionLostEventFilter) {
            return false;
        }

        // below, order of ifs are important.
        // QueryEventFilter is instance of EntryEventFilter.
        // SyntheticEventFilter wraps an event filter.
        if (filter instanceof EventListenerFilter) {
            if (!filter.eval(eventType.getType())) {
                return false;
            } else {
                filter = ((EventListenerFilter) filter).getEventFilter();
            }
        }

        if (filter instanceof SyntheticEventFilter) {
            if (syntheticEvent) {
                return false;
            } else {
                filter = ((SyntheticEventFilter) filter).getFilter();
            }
        }

        if (filter instanceof TrueEventFilter) {
            return true;
        }

        if (filter instanceof QueryEventFilter) {
            return processQueryEventFilter(filter, eventType, dataKey, dataOldValue, dataValue);
        }

        if (filter instanceof EntryEventFilter) {
            return processEntryEventFilter(filter, dataKey);
        }

        throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }
    //CHECKSTYLE:ON

    protected boolean isIncludeValue(EventFilter filter) {
        // below, order of ifs are important.
        // QueryEventFilter is instance of EntryEventFilter.
        // SyntheticEventFilter wraps an event filter.
        if (filter instanceof EventListenerFilter) {
            filter = ((EventListenerFilter) filter).getEventFilter();
        }

        if (filter instanceof SyntheticEventFilter) {
            filter = ((SyntheticEventFilter) filter).getFilter();
        }

        if (filter instanceof TrueEventFilter) {
            return true;
        }

        if (filter instanceof QueryEventFilter) {
            return ((QueryEventFilter) filter).isIncludeValue();
        }

        if (filter instanceof EntryEventFilter) {
            return ((EntryEventFilter) filter).isIncludeValue();
        }

        throw new IllegalArgumentException("Unknown EventFilter type = [" + filter.getClass().getCanonicalName() + "]");
    }

    @Override
    public void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId) {
        Collection<EventRegistration> registrations = new LinkedList<EventRegistration>();
        for (EventRegistration registration : getRegistrations(mapName)) {
            if (registration.getFilter() instanceof MapPartitionLostEventFilter) {
                registrations.add(registration);
            }
        }

        if (registrations.isEmpty()) {
            return;
        }

        String thisNodesAddress = getThisNodesAddress();
        MapPartitionEventData eventData = new MapPartitionEventData(thisNodesAddress, mapName, caller, partitionId);
        publishEventInternal(registrations, eventData, partitionId);
    }

    @Override
    public void hintMapEvent(Address caller, String mapName, EntryEventType eventType,
                             int numberOfEntriesAffected, int partitionId) {
        // NOP
    }

    protected Collection<EventRegistration> getRegistrations(String mapName) {
        return eventService.getRegistrations(SERVICE_NAME, mapName);
    }

    private int pickOrderKey(Data key) {
        return key == null ? -1 : key.hashCode();
    }

    protected void publishEventInternal(Collection<EventRegistration> registrations, Object eventData, int orderKey) {
        eventService.publishEvent(SERVICE_NAME, registrations, eventData, orderKey);
    }

    private String getThisNodesAddress() {
        Address thisAddress = nodeEngine.getThisAddress();
        return thisAddress.toString();
    }

    private boolean processEntryEventFilter(EventFilter filter, Data dataKey) {
        EntryEventFilter eventFilter = (EntryEventFilter) filter;
        return eventFilter.eval(dataKey);
    }

    private boolean processQueryEventFilter(EventFilter filter, EntryEventType eventType,
                                            Data dataKey, Data dataOldValue, Data dataValue) {
        Data testValue;
        if (eventType == REMOVED || eventType == EVICTED || eventType == EXPIRED) {
            testValue = dataOldValue;
        } else {
            testValue = dataValue;
        }

        QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
        QueryableEntry entry = mapServiceContext.newQueryEntry(dataKey, dataKey, testValue);
        return queryEventFilter.eval(entry);
    }

    protected void publishWanReplicationEventInternal(String mapName, ReplicationEventObject event) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        WanReplicationPublisher wanReplicationPublisher = mapContainer.getWanReplicationPublisher();
        wanReplicationPublisher.publishReplicationEvent(SERVICE_NAME, event);
    }

    private EntryEventData createEntryEventData(String mapName, Address caller,
                                                Data dataKey, Data dataNewValue, Data dataOldValue,
                                                Data dataMergingValue, int eventType) {
        String thisNodesAddress = getThisNodesAddress();
        return new EntryEventData(thisNodesAddress, mapName, caller,
                dataKey, dataNewValue, dataOldValue, dataMergingValue, eventType);
    }

}

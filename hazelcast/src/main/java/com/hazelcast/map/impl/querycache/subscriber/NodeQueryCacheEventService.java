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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.QueryCacheListenerAdapter;
import com.hazelcast.map.impl.querycache.event.LocalCacheWideEventData;
import com.hazelcast.map.impl.querycache.event.LocalEntryEventData;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;

import static com.hazelcast.map.impl.querycache.ListenerRegistrationHelper.generateListenerName;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdaptor;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Node side event service implementation for query cache.
 *
 * @see QueryCacheEventService
 */
public class NodeQueryCacheEventService implements QueryCacheEventService<EventData> {

    private final MapServiceContext mapServiceContext;

    public NodeQueryCacheEventService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    // TODO not used order key
    @Override
    public void publish(String mapName, String cacheName, EventData eventData, int orderKey) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheName, "cacheName");
        checkNotNull(eventData, "eventData cannot be null");

        publishLocalEvent(mapName, cacheName, eventData);
    }

    @Override
    public String addListener(String mapName, String cacheName, MapListener listener) {
        return addListener(mapName, cacheName, listener, null);
    }

    @Override
    public String listenPublisher(String mapName, String cacheName, ListenerAdapter listenerAdapter) {
        String listenerName = generateListenerName(mapName, cacheName);
        return mapServiceContext.addListenerAdapter(listenerName, listenerAdapter);
    }

    @Override
    public boolean removePublisherListener(String mapName, String listenerId) {
        return mapServiceContext.removeEventListener(mapName, listenerId);
    }

    @Override
    public String addListener(String mapName, String cacheName, MapListener listener, EventFilter filter) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheName, "cacheName");
        checkNotNull(listener, "listener cannot be null");

        ListenerAdapter queryCacheListenerAdaptor = createQueryCacheListenerAdaptor(listener);
        ListenerAdapter listenerAdaptor = new SimpleQueryCacheListenerAdapter(queryCacheListenerAdaptor);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EventService eventService = nodeEngine.getEventService();

        String listenerName = generateListenerName(mapName, cacheName);
        EventRegistration registration;
        if (filter == null) {
            registration = eventService.registerLocalListener(MapService.SERVICE_NAME,
                    listenerName, listenerAdaptor);
        } else {
            registration = eventService.registerLocalListener(MapService.SERVICE_NAME,
                    listenerName, filter, listenerAdaptor);
        }
        return registration.getId();
    }

    @Override
    public boolean removeListener(String mapName, String cacheName, String id) {
        String listenerName = generateListenerName(mapName, cacheName);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(MapService.SERVICE_NAME, listenerName, id);
    }

    @Override
    public boolean hasListener(String mapName, String cacheName) {
        String listenerName = generateListenerName(mapName, cacheName);
        Collection<EventRegistration> eventRegistrations = getRegistrations(listenerName);
        if (eventRegistrations.isEmpty()) {
            return false;
        }

        for (EventRegistration eventRegistration : eventRegistrations) {
            Registration registration = (Registration) eventRegistration;
            Object listener = registration.getListener();
            if (listener instanceof QueryCacheListenerAdapter) {
                return true;
            }
        }

        return false;
    }

    // TODO needs refactoring.
    private void publishLocalEvent(String mapName, String cacheName, Object eventData) {
        String listenerName = generateListenerName(mapName, cacheName);
        Collection<EventRegistration> eventRegistrations = getRegistrations(listenerName);
        if (eventRegistrations.isEmpty()) {
            return;
        }

        for (EventRegistration eventRegistration : eventRegistrations) {
            Registration registration = (Registration) eventRegistration;
            Object listener = registration.getListener();
            if (!(listener instanceof QueryCacheListenerAdapter)) {
                continue;
            }
            Object eventDataToPublish = eventData;
            int orderKey = -1;
            if (eventDataToPublish instanceof LocalCacheWideEventData) {
                orderKey = listenerName.hashCode();
            } else if (eventDataToPublish instanceof LocalEntryEventData) {
                LocalEntryEventData localEntryEventData = (LocalEntryEventData) eventDataToPublish;
                if (localEntryEventData.getEventType() != EventLostEvent.EVENT_TYPE) {
                    EventFilter filter = registration.getFilter();
                    if (!canPassFilter(localEntryEventData, filter)) {
                        continue;
                    } else {
                        boolean includeValue = isIncludeValue(filter);
                        eventDataToPublish = includeValue ? localEntryEventData : localEntryEventData.cloneWithoutValue();
                        Data keyData = localEntryEventData.getKeyData();
                        orderKey = keyData == null ? -1 : keyData.hashCode();
                    }
                }
            }

            publishEventInternal(registration, eventDataToPublish, orderKey);
        }
    }

    private boolean canPassFilter(LocalEntryEventData localEntryEventData, EventFilter filter) {
        if (filter == null || filter instanceof TrueEventFilter) {
            return true;
        }

        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();

        Data keyData = localEntryEventData.getKeyData();
        Object value = getValueOrOldValue(localEntryEventData);

        QueryableEntry entry = new QueryEntry((InternalSerializationService) serializationService,
                keyData, value, Extractors.empty());
        return filter.eval(entry);
    }

    private boolean isIncludeValue(EventFilter filter) {
        if (filter instanceof EntryEventFilter) {
            return ((EntryEventFilter) filter).isIncludeValue();
        }
        return true;
    }

    private Object getValueOrOldValue(LocalEntryEventData localEntryEventData) {
        Object value = localEntryEventData.getValue();
        return value != null ? value : localEntryEventData.getOldValue();
    }

    private Collection<EventRegistration> getRegistrations(String mapName) {
        MapServiceContext mapServiceContext = this.mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EventService eventService = nodeEngine.getEventService();

        return eventService.getRegistrations(MapService.SERVICE_NAME, mapName);
    }

    private void publishEventInternal(EventRegistration registration, Object eventData, int orderKey) {
        MapServiceContext mapServiceContext = this.mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EventService eventService = nodeEngine.getEventService();
        eventService.publishEvent(MapService.SERVICE_NAME, registration, eventData, orderKey);
    }

    @Override
    public void sendEventToSubscriber(String name, Object eventData, int orderKey) {
        Collection<EventRegistration> eventRegistrations = getRegistrations(name);
        if (eventRegistrations.isEmpty()) {
            return;
        }
        for (EventRegistration eventRegistration : eventRegistrations) {
            Registration registration = (Registration) eventRegistration;
            Object listener = registration.getListener();
            if (listener instanceof QueryCacheListenerAdapter) {
                continue;
            }
            publishEventInternal(registration, eventData, orderKey);
        }
    }

    /**
     * Listener for a {@link com.hazelcast.map.QueryCache QueryCache}.
     *
     * @see com.hazelcast.core.IMap#getQueryCache(String, MapListener, com.hazelcast.query.Predicate, boolean)
     */
    private static class SimpleQueryCacheListenerAdapter implements QueryCacheListenerAdapter<IMapEvent> {

        private final ListenerAdapter listenerAdapter;

        SimpleQueryCacheListenerAdapter(ListenerAdapter listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        @Override
        public void onEvent(IMapEvent event) {
            listenerAdapter.onEvent(event);
        }
    }
}

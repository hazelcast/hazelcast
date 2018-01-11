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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.ListenerAdapter;
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
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.querycache.ListenerRegistrationHelper.generateListenerName;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdaptor;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Node side event service implementation for query cache.
 *
 * @see QueryCacheEventService
 */
public class NodeQueryCacheEventService implements QueryCacheEventService<EventData> {

    private final EventService eventService;
    private final ContextMutexFactory mutexFactory;
    private final MapServiceContext mapServiceContext;

    public NodeQueryCacheEventService(MapServiceContext mapServiceContext, ContextMutexFactory mutexFactory) {
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.eventService = nodeEngine.getEventService();
        this.mutexFactory = mutexFactory;
    }

    // TODO not used order key
    @Override
    public void publish(String mapName, String cacheId, EventData eventData, int orderKey) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(eventData, "eventData cannot be null");

        publishLocalEvent(mapName, cacheId, eventData);
    }

    @Override
    public String addListener(String mapName, String cacheId, MapListener listener) {
        return addListener(mapName, cacheId, listener, null);
    }

    @Override
    public String addPublisherListener(String mapName, String cacheId, ListenerAdapter listenerAdapter) {
        String listenerName = generateListenerName(mapName, cacheId);
        return mapServiceContext.addListenerAdapter(listenerAdapter, TrueEventFilter.INSTANCE, listenerName);
    }

    @Override
    public boolean removePublisherListener(String mapName, String cacheId, String listenerId) {
        String listenerName = generateListenerName(mapName, cacheId);
        return mapServiceContext.removeEventListener(listenerName, listenerId);
    }

    @Override
    public String addListener(String mapName, String cacheId, MapListener listener, EventFilter filter) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(listener, "listener cannot be null");

        ListenerAdapter queryCacheListenerAdaptor = createQueryCacheListenerAdaptor(listener);
        ListenerAdapter listenerAdaptor = new SimpleQueryCacheListenerAdapter(queryCacheListenerAdaptor);

        String listenerName = generateListenerName(mapName, cacheId);
        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, listenerName,
                        filter == null ? TrueEventFilter.INSTANCE : filter, listenerAdaptor);
                return registration.getId();
            }
        } finally {
            closeResource(mutex);
        }
    }

    @Override
    public boolean removeListener(String mapName, String cacheId, String listenerId) {
        String listenerName = generateListenerName(mapName, cacheId);
        return eventService.deregisterListener(SERVICE_NAME, listenerName, listenerId);
    }

    @Override
    public void removeAllListeners(String mapName, String cacheId) {
        String listenerName = generateListenerName(mapName, cacheId);
        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                eventService.deregisterAllListeners(SERVICE_NAME, listenerName);
            }
        } finally {
            closeResource(mutex);
        }
    }

    @Override
    public boolean hasListener(String mapName, String cacheId) {
        String listenerName = generateListenerName(mapName, cacheId);
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
    private void publishLocalEvent(String mapName, String cacheId, Object eventData) {
        String listenerName = generateListenerName(mapName, cacheId);
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
        return eventService.getRegistrations(SERVICE_NAME, mapName);
    }

    private void publishEventInternal(EventRegistration registration, Object eventData, int orderKey) {
        eventService.publishEvent(SERVICE_NAME, registration, eventData, orderKey);
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

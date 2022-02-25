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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.QueryCacheListenerAdapter;
import com.hazelcast.map.impl.querycache.event.LocalCacheWideEventData;
import com.hazelcast.map.impl.querycache.event.LocalEntryEventData;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ContextMutexFactory;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdaptor;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Node side event service implementation for query cache.
 *
 * @see QueryCacheEventService
 */
public class NodeQueryCacheEventService implements QueryCacheEventService<EventData> {

    private final EventService eventService;
    private final ContextMutexFactory lifecycleMutexFactory;
    private final MapServiceContext mapServiceContext;

    public NodeQueryCacheEventService(MapServiceContext mapServiceContext, ContextMutexFactory lifecycleMutexFactory) {
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.eventService = nodeEngine.getEventService();
        this.lifecycleMutexFactory = lifecycleMutexFactory;
    }

    // TODO not used order key
    @Override
    public void publish(String mapName, String cacheId, EventData eventData,
                        int orderKey, Extractors extractors) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(eventData, "eventData cannot be null");

        publishLocalEvent(cacheId, eventData, extractors);
    }

    @Override
    public UUID addListener(String mapName, String cacheId, MapListener listener) {
        return addListener(mapName, cacheId, listener, null);
    }

    @Override
    public UUID addPublisherListener(String mapName, String cacheId, ListenerAdapter listenerAdapter) {
        return mapServiceContext.addListenerAdapter(listenerAdapter, TrueEventFilter.INSTANCE, cacheId);
    }

    @Override
    public boolean removePublisherListener(String mapName, String cacheId, UUID listenerId) {
        return mapServiceContext.removeEventListener(cacheId, listenerId);
    }

    @Override
    public UUID addListener(String mapName, String cacheId, MapListener listener, EventFilter filter) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(listener, "listener cannot be null");

        ListenerAdapter queryCacheListenerAdaptor = createQueryCacheListenerAdaptor(listener);
        ListenerAdapter listenerAdaptor = new SimpleQueryCacheListenerAdapter(queryCacheListenerAdaptor);

        ContextMutexFactory.Mutex mutex = lifecycleMutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, cacheId,
                        filter == null ? TrueEventFilter.INSTANCE : filter, listenerAdaptor);
                return registration.getId();
            }
        } finally {
            closeResource(mutex);
        }
    }

    @Override
    public boolean removeListener(String mapName, String cacheId, UUID listenerId) {
        return eventService.deregisterListener(SERVICE_NAME, cacheId, listenerId);
    }

    @Override
    public void removeAllListeners(String mapName, String cacheId) {
        ContextMutexFactory.Mutex mutex = lifecycleMutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                eventService.deregisterAllListeners(SERVICE_NAME, cacheId);
            }
        } finally {
            closeResource(mutex);
        }
    }

    @Override
    public boolean hasListener(String mapName, String cacheId) {
        Collection<EventRegistration> eventRegistrations = getRegistrations(cacheId);

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
    private void publishLocalEvent(String cacheId, Object eventData, Extractors extractors) {
        Collection<EventRegistration> eventRegistrations = getRegistrations(cacheId);
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
                orderKey = cacheId.hashCode();
            } else if (eventDataToPublish instanceof LocalEntryEventData) {
                LocalEntryEventData localEntryEventData = (LocalEntryEventData) eventDataToPublish;
                if (localEntryEventData.getEventType() != EventLostEvent.EVENT_TYPE) {
                    EventFilter filter = registration.getFilter();
                    if (!canPassFilter(localEntryEventData, filter, extractors)) {
                        continue;
                    } else {
                        boolean includeValue = isIncludeValue(filter);
                        eventDataToPublish = includeValue
                                ? localEntryEventData : localEntryEventData.cloneWithoutValue();
                        Data keyData = localEntryEventData.getKeyData();
                        orderKey = keyData == null ? -1 : keyData.hashCode();
                    }
                }
            }

            publishEventInternal(registration, eventDataToPublish, orderKey);
        }
    }

    private boolean canPassFilter(LocalEntryEventData localEntryEventData,
                                  EventFilter filter, Extractors extractors) {
        if (filter == null || filter instanceof TrueEventFilter) {
            return true;
        }

        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();

        Data keyData = localEntryEventData.getKeyData();
        Object value = getValueOrOldValue(localEntryEventData);

        QueryableEntry entry = new QueryEntry(serializationService, keyData, value, extractors);
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
     * @see IMap#getQueryCache(String, MapListener, com.hazelcast.query.Predicate, boolean)
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

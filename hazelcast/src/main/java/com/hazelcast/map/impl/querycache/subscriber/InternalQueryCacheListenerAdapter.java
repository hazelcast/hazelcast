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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.listener.MapListener;

import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdapters;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Internal listener adapter for the listener of a {@code QueryCache}.
 */
class InternalQueryCacheListenerAdapter implements ListenerAdapter<IMapEvent> {

    private final ListenerAdapter[] listenerAdapters;

    InternalQueryCacheListenerAdapter(MapListener mapListener) {
        checkNotNull(mapListener, "mapListener cannot be null");

        this.listenerAdapters = createQueryCacheListenerAdapters(mapListener);
    }

    @Override
    public void onEvent(IMapEvent event) {
        EntryEventType eventType = event.getEventType();

        if (eventType != null) {
            callListener(event, eventType.getType());
            return;
        }

        if (event instanceof EventLostEvent) {
            EventLostEvent eventLostEvent = (EventLostEvent) event;
            callListener(eventLostEvent, EventLostEvent.EVENT_TYPE);
            return;
        }
    }

    private void callListener(IMapEvent event, int eventType) {
        int adapterIndex = Integer.numberOfTrailingZeros(eventType);
        ListenerAdapter listenerAdapter = listenerAdapters[adapterIndex];
        if (listenerAdapter == null) {
            return;
        }
        listenerAdapter.onEvent(event);
    }
}

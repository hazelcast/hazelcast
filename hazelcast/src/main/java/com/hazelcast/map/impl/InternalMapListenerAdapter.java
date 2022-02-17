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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.listener.MapListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.map.impl.MapListenerAdaptors.createListenerAdapters;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Internal-usage-only adapter which wraps all {@link MapListener} sub-interfaces into a {@link ListenerAdapter}.
 * <p>
 * Main purpose of this adapter is to avoid lots of instanceOf checks when firing events to check whether or not
 * a corresponding {@link MapListener} sub-interface for a specific {@link com.hazelcast.core.EntryEventType} is extended.
 * And also to provide an abstraction over all {@link MapListener} sub-interfaces to make a smooth usage when passing
 * fired events to listeners e.g. only calling {@link ListenerAdapter#onEvent} is sufficient to fire any event.
 */
public class InternalMapListenerAdapter implements ListenerAdapter<IMapEvent> {

    private final ListenerAdapter[] listenerAdapters;

    InternalMapListenerAdapter(MapListener mapListener) {
        isNotNull(mapListener, "mapListener");

        this.listenerAdapters = createListenerAdapters(mapListener);
    }

    @Override
    public void onEvent(IMapEvent event) {
        EntryEventType eventType = event.getEventType();
        if (eventType == null) {
            return;
        }

        ListenerAdapter listenerAdapter = listenerAdapters[eventType.ordinal()];
        if (listenerAdapter == null) {
            return;
        }
        listenerAdapter.onEvent(event);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "listenerAdapters internal state is never changed")
    public ListenerAdapter[] getListenerAdapters() {
        return listenerAdapters;
    }
}


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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Internal-usage-only adapter which wraps all {@link EntryListener} interfaces
 * into a {@link com.hazelcast.map.impl.ListenerAdapter}.
 * <p/>
 * Main purpose of this adapter is to avoid backward compatibility problems
 * when one doesn't use a {@link com.hazelcast.map.listener.MapListener MapListener}
 */
class InternalEntryListenerAdapter implements ListenerAdapter<IMapEvent> {

    private final ListenerAdapter[] listenerAdapters;

    InternalEntryListenerAdapter(EntryListener listener) {
        isNotNull(listener, "listener");

        this.listenerAdapters = EntryListenerAdaptors.createListenerAdapters(listener);
    }

    @Override
    public void onEvent(IMapEvent event) {
        final EntryEventType eventType = event.getEventType();
        final ListenerAdapter listenerAdapter = listenerAdapters[eventType.ordinal()];
        if (listenerAdapter == null) {
            return;
        }
        listenerAdapter.onEvent(event);
    }
}


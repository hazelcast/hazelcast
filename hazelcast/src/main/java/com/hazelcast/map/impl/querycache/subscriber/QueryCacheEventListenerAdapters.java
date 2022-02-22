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
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapListenerAdaptors;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.internal.util.ConstructorFunction;

/**
 * Responsible for creating {@link com.hazelcast.map.QueryCache QueryCache}
 * specific implementations of listeners.
 */
public final class QueryCacheEventListenerAdapters {

    /**
     * Converts an {@link com.hazelcast.map.listener.EventLostListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> EVENT_LOST_LISTENER_ADAPTER =
            mapListener -> {
                if (!(mapListener instanceof EventLostListener)) {
                    return null;
                }
                final EventLostListener listener = (EventLostListener) mapListener;
                return (ListenerAdapter<IMapEvent>) iMapEvent -> listener.eventLost((EventLostEvent) iMapEvent);
            };

    private QueryCacheEventListenerAdapters() {
    }

    static ListenerAdapter[] createQueryCacheListenerAdapters(MapListener mapListener) {
        ListenerAdapter[] mapListenerAdapters = MapListenerAdaptors.createListenerAdapters(mapListener);
        ListenerAdapter eventLostAdapter = EVENT_LOST_LISTENER_ADAPTER.createNew(mapListener);
        ListenerAdapter[] adapters = new ListenerAdapter[mapListenerAdapters.length + 1];
        System.arraycopy(mapListenerAdapters, 0, adapters, 0, mapListenerAdapters.length);
        adapters[mapListenerAdapters.length] = eventLostAdapter;
        return adapters;
    }

    /**
     * Wraps a user defined {@link com.hazelcast.map.listener.MapListener}
     * into a {@link com.hazelcast.map.impl.ListenerAdapter}.
     *
     * @param mapListener a {@link com.hazelcast.map.listener.MapListener} instance.
     * @return {@link com.hazelcast.map.impl.ListenerAdapter} for the user-defined
     * {@link com.hazelcast.map.listener.MapListener}
     */
    public static ListenerAdapter createQueryCacheListenerAdaptor(MapListener mapListener) {
        return new InternalQueryCacheListenerAdapter(mapListener);
    }
}

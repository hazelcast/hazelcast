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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.util.ConstructorFunction;

import java.util.EnumMap;
import java.util.Map;

/**
 * Used to support deprecated {@link com.hazelcast.core.IMap IMap} listener related methods
 * such as {@link com.hazelcast.core.IMap#addLocalEntryListener(EntryListener)}.
 * <p/>
 * This is a static factory class which creates various
 * {@link com.hazelcast.map.impl.ListenerAdapter} implementations.
 */
public final class EntryListenerAdaptors {

    /**
     * Registry for all {@link EntryListener} to {@link com.hazelcast.map.impl.ListenerAdapter}
     * constructors according to {@link com.hazelcast.core.EntryEventType}s.
     */
    private static final Map<EntryEventType, ConstructorFunction<EntryListener, ListenerAdapter>> CONSTRUCTORS
            = new EnumMap<EntryEventType, ConstructorFunction<EntryListener, ListenerAdapter>>(EntryEventType.class);

    /**
     * Converts an {@link EntryListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<EntryListener, ListenerAdapter> ENTRY_ADDED_LISTENER_ADAPTER_CONSTRUCTOR =
            new ConstructorFunction<EntryListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(final EntryListener listener) {
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent event) {
                            listener.entryAdded((EntryEvent) event);
                        }
                    };
                }
            };

    /**
     * Converts an {@link EntryListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<EntryListener, ListenerAdapter> ENTRY_REMOVED_LISTENER_ADAPTER_CONSTRUCTOR =
            new ConstructorFunction<EntryListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(final EntryListener listener) {
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent event) {
                            listener.entryRemoved((EntryEvent) event);
                        }
                    };
                }
            };


    /**
     * Converts an {@link EntryListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<EntryListener, ListenerAdapter> ENTRY_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR =
            new ConstructorFunction<EntryListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(final EntryListener listener) {
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent event) {
                            listener.entryEvicted((EntryEvent) event);
                        }
                    };
                }
            };


    /**
     * Converts an {@link EntryListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<EntryListener, ListenerAdapter> ENTRY_UPDATED_LISTENER_ADAPTER_CONSTRUCTOR =
            new ConstructorFunction<EntryListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(final EntryListener listener) {
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent event) {
                            listener.entryUpdated((EntryEvent) event);
                        }
                    };
                }
            };


    /**
     * Converts an {@link EntryListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<EntryListener, ListenerAdapter> MAP_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR =
            new ConstructorFunction<EntryListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(final EntryListener listener) {
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent event) {
                            listener.mapEvicted((MapEvent) event);
                        }
                    };
                }
            };


    /**
     * Converts an {@link EntryListener} to a {@link com.hazelcast.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<EntryListener, ListenerAdapter> MAP_CLEARED_LISTENER_ADAPTER_CONSTRUCTOR =
            new ConstructorFunction<EntryListener, ListenerAdapter>() {
                @Override
                public ListenerAdapter createNew(final EntryListener listener) {
                    return new ListenerAdapter() {
                        @Override
                        public void onEvent(IMapEvent event) {
                            listener.mapCleared((MapEvent) event);
                        }
                    };
                }
            };

    /**
     * Register all {@link com.hazelcast.map.impl.ListenerAdapter} constructors
     * according to {@link com.hazelcast.core.EntryEventType}s.
     */
    static {
        CONSTRUCTORS.put(EntryEventType.ADDED, ENTRY_ADDED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.REMOVED, ENTRY_REMOVED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.EVICTED, ENTRY_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.UPDATED, ENTRY_UPDATED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.EVICT_ALL, MAP_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.CLEAR_ALL, MAP_CLEARED_LISTENER_ADAPTER_CONSTRUCTOR);
    }

    private EntryListenerAdaptors() {
    }

    /**
     * Creates a {@link com.hazelcast.map.impl.ListenerAdapter} array
     * for all event types of {@link com.hazelcast.core.EntryEventType}.
     *
     * @param listener a {@link EntryListener} instance.
     * @return an array of {@link com.hazelcast.map.impl.ListenerAdapter}
     */
    public static ListenerAdapter[] createListenerAdapters(EntryListener listener) {
        EntryEventType[] values = EntryEventType.values();
        ListenerAdapter[] listenerAdapters = new ListenerAdapter[values.length];
        for (EntryEventType eventType : values) {
            listenerAdapters[eventType.ordinal()] = createListenerAdapter(eventType, listener);
        }
        return listenerAdapters;
    }

    /**
     * Creates a {@link ListenerAdapter} for a specific {@link com.hazelcast.core.EntryEventType}.
     *
     * @param eventType an {@link com.hazelcast.core.EntryEventType}.
     * @param listener  a {@link EntryListener} instance.
     * @return {@link com.hazelcast.map.impl.ListenerAdapter} for a specific {@link com.hazelcast.core.EntryEventType}
     */
    private static ListenerAdapter createListenerAdapter(EntryEventType eventType, EntryListener listener) {
        final ConstructorFunction<EntryListener, ListenerAdapter> constructorFunction = CONSTRUCTORS.get(eventType);
        if (constructorFunction == null) {
            throw new IllegalArgumentException("First, define a ListenerAdapter for the event EntryEventType." + eventType);
        }
        return constructorFunction.createNew(listener);
    }


    /**
     * Wraps a user defined {@link EntryListener}
     * into a {@link com.hazelcast.map.impl.ListenerAdapter}.
     *
     * @param listener a {@link EntryListener} instance.
     * @return {@link com.hazelcast.map.impl.ListenerAdapter} for the user-defined
     * {@link com.hazelcast.map.listener.MapListener}
     */
    static ListenerAdapter createEntryListenerAdaptor(EntryListener listener) {
        return new InternalEntryListenerAdapter(listener);
    }

}

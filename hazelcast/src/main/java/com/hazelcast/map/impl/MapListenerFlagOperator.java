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

/**
 * Contains helper methods to set bit flags of implemented listener interfaces
 * of a {@link com.hazelcast.map.listener.MapListener MapListener}.
 */
public final class MapListenerFlagOperator {

    /**
     * Sets all listener flags. Used to be notified for all events. No filtering.
     */
    public static final int SET_ALL_LISTENER_FLAGS = setAndGetAllListenerFlags();

    private MapListenerFlagOperator() {
    }

    /**
     * Sets and gets flags of implemented listener interfaces of a {@link com.hazelcast.map.listener.MapListener MapListener}.
     */
    public static int setAndGetListenerFlags(ListenerAdapter listenerAdapter) {
        InternalMapListenerAdapter internalMapListenerAdapter = (InternalMapListenerAdapter) listenerAdapter;
        ListenerAdapter[] listenerAdapters = internalMapListenerAdapter.getListenerAdapters();
        EntryEventType[] values = EntryEventType.values();

        int listenerFlags = 0;

        for (EntryEventType eventType : values) {
            ListenerAdapter definedAdapter = listenerAdapters[eventType.ordinal()];
            if (definedAdapter != null) {
                listenerFlags = listenerFlags | eventType.getType();
            }
        }
        return listenerFlags;
    }

    /**
     * Sets and gets all listener flags.
     */
    private static int setAndGetAllListenerFlags() {
        int listenerFlags = 0;
        EntryEventType[] values = EntryEventType.values();
        for (EntryEventType eventType : values) {
            listenerFlags = listenerFlags | eventType.getType();
        }
        return listenerFlags;
    }
}

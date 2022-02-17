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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;

import java.io.IOException;

import static com.hazelcast.map.impl.MapListenerFlagOperator.SET_ALL_LISTENER_FLAGS;

/**
 * Event filter matching events of specified types. This filter also contains an another filter but it does not
 * involve it when evaluating if the event matches.
 * Prevents sending of not requested events to a {@link com.hazelcast.map.listener.MapListener MapListener}
 * by filtering events according the implemented {@link com.hazelcast.map.listener.MapListener MapListener} sub-interfaces.
 * <p>
 * Specifically, for example, if a listener is registered via an implementation like this:
 * <p>
 * {@code}
 * public class MyMapListener implements EntryAddedListener, EntryRemovedListener {
 * ...
 * }
 * {@code}
 * <p>
 * That listener will only be notified for {@link EntryEventType#ADDED} and {@link EntryEventType#REMOVED} events.
 * Other events, like {@link EntryEventType#EVICTED} or {@link EntryEventType#EXPIRED}, will not be sent over wire.
 * This may help to reduce load on eventing system and network.
 *
 * @see MapListenerFlagOperator#setAndGetListenerFlags(ListenerAdapter)
 * @see com.hazelcast.map.listener.MapListener
 * @since 3.6
 */
public class EventListenerFilter implements EventFilter, IdentifiedDataSerializable {

    /**
     * Flags of implemented listeners.
     */
    private int listenerFlags;
    private EventFilter eventFilter;

    public EventListenerFilter() {
        this(SET_ALL_LISTENER_FLAGS, TrueEventFilter.INSTANCE);
    }

    public EventListenerFilter(int listenerFlags, EventFilter eventFilter) {
        this.listenerFlags = listenerFlags;
        this.eventFilter = eventFilter == null ? TrueEventFilter.INSTANCE : eventFilter;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(eventFilter);
        out.writeInt(listenerFlags);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventFilter = in.readObject();
        listenerFlags = in.readInt();
    }

    @Override
    public boolean eval(Object object) {
        Integer eventType = (Integer) object;
        return (listenerFlags & eventType) != 0;
    }

    public EventFilter getEventFilter() {
        return eventFilter;
    }

    @Override
    public String toString() {
        return "EventListenerFilter{"
                + "listenerFlags=" + listenerFlags
                + ", eventFilter=" + eventFilter
                + '}';
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVENT_LISTENER_FILTER;
    }
}

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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableEntryEvent;

public class ClientMapListener extends MapListenerAdapter<Object, Object> {

    private final long callId;
    private final ClientEndpoint endpoint;

    public ClientMapListener(ClientEndpoint endpoint, long callId) {
        this.endpoint = endpoint;
        this.callId = callId;
    }

    @Override
    public void onEntryEvent(EntryEvent<Object, Object> event) {
        if (endpoint.isAlive()) {
            if (!(event instanceof DataAwareEntryEvent)) {
                throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                        + event.getClass().getSimpleName());
            }
            DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
            Data key = dataAwareEntryEvent.getKeyData();
            Data value = dataAwareEntryEvent.getNewValueData();
            Data oldValue = dataAwareEntryEvent.getOldValueData();
            Data mergingValue = dataAwareEntryEvent.getMergingValueData();
            PortableEntryEvent portableEntryEvent = new PortableEntryEvent(key, value, oldValue, mergingValue,
                    event.getEventType(), event.getMember().getUuid());
            endpoint.sendEvent(key, portableEntryEvent, callId);
        }
    }

    @Override
    public void onMapEvent(MapEvent event) {
        if (endpoint.isAlive()) {
            final EntryEventType type = event.getEventType();
            final String uuid = event.getMember().getUuid();
            PortableEntryEvent portableEntryEvent =
                    new PortableEntryEvent(type, uuid, event.getNumberOfEntriesAffected());
            endpoint.sendEvent(null, portableEntryEvent, callId);
        }
    }
}

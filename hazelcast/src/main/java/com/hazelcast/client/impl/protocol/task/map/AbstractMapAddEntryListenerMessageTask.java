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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.parameters.EntryEventParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.MapEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.EventFilter;

import java.security.Permission;

import static com.hazelcast.nio.serialization.DefaultData.NULL_DATA;

public abstract class AbstractMapAddEntryListenerMessageTask<Parameter>
        extends AbstractCallableMessageTask<Parameter> {

    public AbstractMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        final ClientEndpoint endpoint = getEndpoint();
        final MapService mapService = getService(MapService.SERVICE_NAME);

        EntryAdapter<Object, Object> listener = new MapListener();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final String name = getDistributedObjectName();
        final String registrationId = mapServiceContext.addEventListener(listener, getEventFilter(), name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, name, registrationId);
        return AddListenerResultParameters.encode(registrationId);
    }

    protected abstract EventFilter getEventFilter();

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    private class MapListener extends EntryAdapter<Object, Object> {

        @Override
        public void onEntryEvent(EntryEvent<Object, Object> event) {
            if (endpoint.isAlive()) {
                if (!(event instanceof DataAwareEntryEvent)) {
                    throw new IllegalArgumentException(
                            "Expecting: DataAwareEntryEvent, Found: " + event.getClass().getSimpleName());
                }
                DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                Data keyData = resolveData(dataAwareEntryEvent.getKeyData());
                Data newValueData = resolveData(dataAwareEntryEvent.getNewValueData());
                Data oldValueData = resolveData(dataAwareEntryEvent.getOldValueData());
                Data meringValueData = resolveData(dataAwareEntryEvent.getMeringValueData());
                ClientMessage entryEvent = EntryEventParameters.encode(keyData
                        , newValueData, oldValueData, meringValueData, event.getEventType().getType(),
                        event.getMember().getUuid(), 1);
                sendClientMessage(entryEvent);
            }
        }

        private Data resolveData(Data data) {
            return data == null ? DefaultData.NULL_DATA : data;
        }

        @Override
        public void onMapEvent(MapEvent event) {
            if (endpoint.isAlive()) {
                final EntryEventType type = event.getEventType();
                final String uuid = event.getMember().getUuid();
                int numberOfEntriesAffected = event.getNumberOfEntriesAffected();
                ClientMessage entryEvent = EntryEventParameters.encode(NULL_DATA,
                        NULL_DATA, NULL_DATA, NULL_DATA, type.getType(), uuid, numberOfEntriesAffected);
                sendClientMessage(entryEvent);
            }
        }
    }
}

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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddEntryListenerEventParameters;
import com.hazelcast.client.impl.protocol.parameters.AddEntryListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.MapEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.QueryEventFilter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.EventFilter;

import java.security.Permission;

public class MapAddEntryListenerTask extends AbstractCallableMessageTask<AddEntryListenerParameters> {

    public MapAddEntryListenerTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected ClientMessage call() {
        final ClientEndpoint endpoint = getEndpoint();
        final MapService mapService = getService(MapService.SERVICE_NAME);

        EntryAdapter<Object, Object> listener = new MapListener();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final String registrationId = mapServiceContext.addEventListener(listener, getEventFilter(), parameters.name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, parameters.name, registrationId);
        return AddListenerResultParameters.encode(registrationId);
    }

    @Override
    protected AddEntryListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return AddEntryListenerParameters.decode(clientMessage);
    }

    protected EventFilter getEventFilter() {
        if (getPredicate() == null) {
            return new EntryEventFilter(parameters.includeValue, parameters.key.length == 0 ? null
                    : new DefaultData(parameters.key));
        }
        return new QueryEventFilter(parameters.includeValue, parameters.key.length == 0 ? null
                : new DefaultData(parameters.key), getPredicate());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    private class MapListener extends EntryAdapter<Object, Object> {

        @Override
        public void onEntryEvent(EntryEvent<Object, Object> event) {
            if (endpoint.isAlive()) {
                if (!(event instanceof DataAwareEntryEvent)) {
                    throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                            + event.getClass().getSimpleName());
                }
                DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                byte[] key = dataAwareEntryEvent.getKeyData() == null ? null
                        : dataAwareEntryEvent.getKeyData().toByteArray();
                byte[] value = dataAwareEntryEvent.getNewValueData() == null ? null
                        : dataAwareEntryEvent.getNewValueData().toByteArray();
                byte[] oldValue = dataAwareEntryEvent.getOldValueData() == null ? null
                        : dataAwareEntryEvent.getOldValueData().toByteArray();
                ClientMessage entryEvent = AddEntryListenerEventParameters.encode(key, value, oldValue,
                        event.getEventType().getType(), event.getMember().getUuid(), 1);
                sendClientMessage(entryEvent);
            }
        }

        @Override
        public void onMapEvent(MapEvent event) {
            if (endpoint.isAlive()) {
                final EntryEventType type = event.getEventType();
                final String uuid = event.getMember().getUuid();
                int numberOfEntriesAffected = event.getNumberOfEntriesAffected();
                ClientMessage entryEvent = AddEntryListenerEventParameters.encode(null, null, null, type.getType(),
                        uuid, numberOfEntriesAffected);
                sendClientMessage(entryEvent);
            }
        }
    }
}

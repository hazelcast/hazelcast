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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.EntryEventParameters;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.MapEvent;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import java.security.Permission;

public abstract class AbstractMultiMapAddEntryListenerMessageTask<P> extends AbstractCallableMessageTask<P> {

    public AbstractMultiMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final MultiMapService service = getService(MultiMapService.SERVICE_NAME);
        EntryAdapter listener = new MultiMapListener();

        final String name = getDistributedObjectName();
        Data key = getKey();
        boolean includeValue = shouldIncludeValue();
        String registrationId = service.addListener(name, listener, key, includeValue, false);
        endpoint.setListenerRegistration(MultiMapService.SERVICE_NAME, name, registrationId);
        return AddListenerResultParameters.encode(registrationId);
    }

    protected abstract boolean shouldIncludeValue();

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }


    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    public Data getKey() {
        return null;
    }

    private class MultiMapListener extends EntryAdapter<Object, Object> {

        @Override
        public void onEntryEvent(EntryEvent event) {
            if (endpoint.isAlive()) {
                if (!(event instanceof DataAwareEntryEvent)) {
                    throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                            + event.getClass().getSimpleName());
                }
                DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                Data key = dataAwareEntryEvent.getKeyData();
                Data value = dataAwareEntryEvent.getNewValueData();
                final EntryEventType type = event.getEventType();
                final String uuid = event.getMember().getUuid();

                ClientMessage entryEvent = EntryEventParameters.encode(key, value, DefaultData.NULL_DATA,
                        DefaultData.NULL_DATA, type.getType(), uuid, 1);
                sendClientMessage(entryEvent);
            }
        }

        @Override
        public void onMapEvent(MapEvent event) {
            if (endpoint.isAlive()) {
                final EntryEventType type = event.getEventType();
                final String uuid = event.getMember().getUuid();
                ClientMessage entryEvent = EntryEventParameters.encode(DefaultData.NULL_DATA,
                        DefaultData.NULL_DATA, DefaultData.NULL_DATA, DefaultData.NULL_DATA, type.getType(),
                        uuid, event.getNumberOfEntriesAffected());
                sendClientMessage(entryEvent);
            }
        }
    }
}

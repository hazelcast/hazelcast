/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.ListenerMessageTask;
import com.hazelcast.collection.impl.collection.CollectionEventFilter;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;

public class ListAddListenerMessageTask
        extends AbstractCallableMessageTask<ListAddListenerCodec.RequestParameters> implements ListenerMessageTask {

    public ListAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        Data partitionKey = serializationService.toData(parameters.name);
        ItemListener listener = createItemListener(endpoint, partitionKey);
        EventService eventService = clientEngine.getEventService();
        CollectionEventFilter filter = new CollectionEventFilter(parameters.includeValue);
        EventRegistration registration;
        if (parameters.localOnly) {
            registration = eventService.registerLocalListener(getServiceName(), parameters.name, filter, listener);
        } else {
            registration = eventService.registerListener(getServiceName(), parameters.name, filter, listener);
        }
        UUID registrationId = registration.getId();
        endpoint.addListenerDestroyAction(getServiceName(), parameters.name, registrationId);
        return registrationId;
    }

    private ItemListener createItemListener(final ClientEndpoint endpoint, final Data partitionKey) {
        return new ItemListener() {

            @Override
            public void itemAdded(ItemEvent item) {
                send(item);
            }

            @Override
            public void itemRemoved(ItemEvent item) {
                send(item);
            }

            private void send(ItemEvent event) {
                if (endpoint.isAlive()) {
                    if (!(event instanceof DataAwareItemEvent)) {
                        throw new IllegalArgumentException(
                                "Expecting: DataAwareItemEvent, Found: " + event.getClass().getSimpleName());
                    }

                    DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) event;
                    Data item = dataAwareItemEvent.getItemData();
                    ClientMessage clientMessage = ListAddListenerCodec
                            .encodeItemEvent(item, event.getMember().getUuid(), event.getEventType().getType());
                    sendClientMessage(partitionKey, clientMessage);
                }
            }
        };
    }

    @Override
    protected ListAddListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListAddListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ListAddListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.includeValue};
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "addItemListener";
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}

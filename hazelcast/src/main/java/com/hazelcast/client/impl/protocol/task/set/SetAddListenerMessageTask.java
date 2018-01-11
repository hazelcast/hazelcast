/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.set;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SetAddListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.collection.impl.collection.CollectionEventFilter;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;

import java.security.Permission;

/**
 * SetAddListenerMessageTask
 */
public class SetAddListenerMessageTask
        extends AbstractCallableMessageTask<SetAddListenerCodec.RequestParameters> {

    public SetAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() {
        ClientEndpoint endpoint = getEndpoint();
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
        String registrationId = registration.getId();
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
                        throw new IllegalArgumentException("Expecting: DataAwareItemEvent, Found: "
                                + event.getClass().getSimpleName());
                    }

                    DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) event;
                    Data item = dataAwareItemEvent.getItemData();
                    ClientMessage clientMessage =
                            SetAddListenerCodec.encodeItemEvent(item, event.getMember().getUuid()
                                    , event.getEventType().getType());
                    sendClientMessage(partitionKey, clientMessage);
                }
            }
        };
    }


    @Override
    protected SetAddListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SetAddListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SetAddListenerCodec.encodeResponse((String) response);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.includeValue};
    }

    @Override
    public Permission getRequiredPermission() {
        return new SetPermission(parameters.name, ActionConstants.ACTION_LISTEN);
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

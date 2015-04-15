/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AddListenerResultParameters;
import com.hazelcast.client.impl.protocol.parameters.ListAddListenerParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.collection.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.collection.CollectionEventFilter;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.impl.PortableItemEvent;
import java.security.Permission;

/**
 * ListAddListenerMessageTask
 */
public class ListAddListenerMessageTask
        extends AbstractCallableMessageTask<ListAddListenerParameters> {

    public ListAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        final ClientEndpoint endpoint = getEndpoint();
        Data partitionKey = serializationService.toData(parameters.name);
        ItemListener listener = createItemListener(endpoint, partitionKey);
        final EventService eventService = clientEngine.getEventService();
        final CollectionEventFilter filter = new CollectionEventFilter(parameters.includeValue);
        final EventRegistration registration = eventService.registerListener(getServiceName(), parameters.name
                , filter, listener);
        final String registrationId = registration.getId();
        endpoint.setListenerRegistration(getServiceName(), parameters.name, registrationId);
        return AddListenerResultParameters.encode(registrationId);
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
                    PortableItemEvent portableItemEvent = new PortableItemEvent(item, event.getEventType(),
                            event.getMember().getUuid());
                    endpoint.sendEvent(partitionKey, portableItemEvent, clientMessage.getCorrelationId());
                }
            }
        };
    }


    @Override
    protected ListAddListenerParameters decodeClientMessage(ClientMessage clientMessage) {
        return ListAddListenerParameters.decode(clientMessage);
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

}

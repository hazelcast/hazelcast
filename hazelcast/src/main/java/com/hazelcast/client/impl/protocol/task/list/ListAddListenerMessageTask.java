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

package com.hazelcast.client.impl.protocol.task.list;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ListAddListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAddListenerMessageTask;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.collection.CollectionEventFilter;
import com.hazelcast.collection.impl.common.DataAwareItemEvent;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class ListAddListenerMessageTask
        extends AbstractAddListenerMessageTask<ListAddListenerCodec.RequestParameters> {

    public ListAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        Data partitionKey = serializationService.toData(parameters.name);
        ItemListener listener = createItemListener(endpoint, partitionKey);
        EventService eventService = clientEngine.getEventService();
        CollectionEventFilter filter = new CollectionEventFilter(parameters.includeValue);
        if (parameters.localOnly) {
            return newCompletedFuture(
                    eventService.registerLocalListener(getServiceName(), parameters.name, filter, listener).getId());
        }

        return eventService.registerListenerAsync(getServiceName(), parameters.name, filter, listener)
                           .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
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

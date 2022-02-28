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

package com.hazelcast.cp.internal.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAsyncMessageTask;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.cp.internal.RaftService.EVENT_TOPIC_AVAILABILITY;

public class RemoveCPGroupAvailabilityListenerMessageTask extends AbstractAsyncMessageTask<UUID, Boolean> {

    public RemoveCPGroupAvailabilityListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Boolean> processInternal() {
        UUID registrationId = parameters;
        endpoint.removeDestroyAction(registrationId);
        EventService eventService = clientEngine.getEventService();
        return eventService.deregisterListenerAsync(getServiceName(), EVENT_TOPIC_AVAILABILITY, registrationId);
    }

    @Override
    protected UUID decodeClientMessage(ClientMessage clientMessage) {
        return CPSubsystemRemoveGroupAvailabilityListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSubsystemRemoveGroupAvailabilityListenerCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    public Permission getRequiredPermission() {
        return null;
    }
}

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

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAsyncMessageTask;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec.encodeMembershipEventEvent;
import static com.hazelcast.cp.internal.RaftService.EVENT_TOPIC_MEMBERSHIP;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;

public class AddCPMembershipListenerMessageTask extends AbstractAsyncMessageTask<Boolean, UUID> {

    private static final String TOPIC = EVENT_TOPIC_MEMBERSHIP;

    public AddCPMembershipListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        EventService eventService = clientEngine.getEventService();
        CPMembershipListener listener = new ClientCPMembershipListener(endpoint);

        boolean local = parameters;
        if (local) {
            UUID id = eventService.registerLocalListener(getServiceName(), TOPIC, listener).getId();
            return CompletableFuture.completedFuture(id);
        }
        return eventService.registerListenerAsync(getServiceName(), TOPIC, listener)
                .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    private class ClientCPMembershipListener implements CPMembershipListener {
        private final ClientEndpoint endpoint;

        ClientCPMembershipListener(ClientEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void memberAdded(CPMembershipEvent event) {
            sendEvent(event);
        }

        @Override
        public void memberRemoved(CPMembershipEvent event) {
            sendEvent(event);
        }

        private void sendEvent(CPMembershipEvent event) {
            if (!endpoint.isAlive()) {
                return;
            }
            ClientMessage message = encodeMembershipEventEvent(event.getMember(), event.getType().id());
            sendClientMessage(message);
        }
    }

    @Override
    protected Object processResponseBeforeSending(UUID registrationId) {
        endpoint.addListenerDestroyAction(getServiceName(), TOPIC, registrationId);
        return registrationId;
    }

    @Override
    protected Boolean decodeClientMessage(ClientMessage clientMessage) {
        return CPSubsystemAddMembershipListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSubsystemAddMembershipListenerCodec.encodeResponse((UUID) response);
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

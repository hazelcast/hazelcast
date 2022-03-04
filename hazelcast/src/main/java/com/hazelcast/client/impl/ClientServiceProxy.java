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

package com.hazelcast.client.impl;

import com.hazelcast.client.Client;
import com.hazelcast.client.ClientListener;
import com.hazelcast.client.ClientService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * The default implementation of the {@link ClientService}.
 */
public final class ClientServiceProxy implements ClientService {

    private final ClientEngine clientEngine;
    private final NodeEngine nodeEngine;

    public ClientServiceProxy(Node node) {
        this.clientEngine = node.clientEngine;
        this.nodeEngine = node.nodeEngine;
    }

    @Nonnull
    @Override
    public Collection<Client> getConnectedClients() {
        return clientEngine.getClients();
    }

    @Nonnull
    @Override
    public UUID addClientListener(@Nonnull ClientListener clientListener) {
        checkNotNull(clientListener, "clientListener should not be null");

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerLocalListener(
                ClientEngineImpl.SERVICE_NAME, ClientEngineImpl.SERVICE_NAME, clientListener);
        return registration.getId();
    }

    @Override
    public boolean removeClientListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId should not be null");

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(
                ClientEngineImpl.SERVICE_NAME, ClientEngineImpl.SERVICE_NAME, registrationId);
    }
}

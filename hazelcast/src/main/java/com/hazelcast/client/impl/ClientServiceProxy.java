/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;

public final class ClientServiceProxy implements ClientService {

    private final ClientEngineImpl clientEngine;
    private final NodeEngine nodeEngine;

    public ClientServiceProxy(Node node) {
        this.clientEngine = node.clientEngine;
        this.nodeEngine = node.nodeEngine;
    }

    @Override
    public Collection<Client> getConnectedClients() {
        return clientEngine.getClients();
    }

    @Override
    public String addClientListener(ClientListener clientListener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService
                .registerLocalListener(ClientEngineImpl.SERVICE_NAME, ClientEngineImpl.SERVICE_NAME, clientListener);
        return registration.getId();
    }

    @Override
    public boolean removeClientListener(String registrationId) {
        final EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(ClientEngineImpl.SERVICE_NAME,
                ClientEngineImpl.SERVICE_NAME, registrationId);
    }
}

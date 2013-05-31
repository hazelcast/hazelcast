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

package com.hazelcast.client;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;

import java.util.Collection;

/**
 * @mdogan 5/14/13
 */
final class ClientServiceProxy implements ClientService {

    private final ClientEngineImpl clientEngine;

    ClientServiceProxy(ClientEngineImpl clientEngine) {
        this.clientEngine = clientEngine;
    }

    public Collection<Client> getConnectedClients() {
        return null;
    }

    public String addClientListener(ClientListener clientListener) {
        return clientEngine.addClientListener(clientListener);
    }

    public boolean removeClientListener(String registrationId) {
        return clientEngine.removeClientListener(registrationId);
    }
}

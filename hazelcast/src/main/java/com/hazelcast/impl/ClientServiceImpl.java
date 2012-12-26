/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.util.ConcurrentHashSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientServiceImpl implements ClientService {
    private final Set<Client> clients = new ConcurrentHashSet<Client>();
    private final List<ClientListener> listeners = new CopyOnWriteArrayList<ClientListener>();
    private final ConcurrentMapManager concurrentMapManager;

    public ClientServiceImpl(ConcurrentMapManager concurrentMapManager) {
        this.concurrentMapManager = concurrentMapManager;
    }

    public Collection<Client> getConnectedClients() {
        return clients;
    }

    public void addClientListener(ClientListener clientListener) {
        listeners.add(clientListener);
    }

    public void removeClientListener(ClientListener clientListener) {
        listeners.remove(clientListener);
    }

    private void doFireClientEvent(final Client client, final boolean connected) {
        if (client == null) throw new IllegalArgumentException("Client is null.");
        for (final ClientListener clientListener : listeners) {
            concurrentMapManager.node.executorManager.executeNow(new Runnable() {
                public void run() {
                    if (connected) {
                        clientListener.clientConnected(client);
                    } else {
                        clientListener.clientDisconnected(client);
                    }
                }
            });
        }
    }

    void add(Client client) {
        this.clients.add(client);
        doFireClientEvent(client, true);
    }

    void remove(Client client) {
        this.clients.remove(client);
        doFireClientEvent(client, false);
    }
}

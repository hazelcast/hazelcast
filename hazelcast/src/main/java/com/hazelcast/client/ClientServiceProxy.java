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

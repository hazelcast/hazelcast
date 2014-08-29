package com.hazelcast.client;

import com.hazelcast.nio.Connection;

import java.util.Collection;
import java.util.Set;

public interface ClientEndpointManager {

    Collection<ClientEndpoint> getEndpoints();

    ClientEndpoint getEndpoint(Connection conn);

    Set<ClientEndpoint> getEndpoints(String clientUuid);

    int size();

    void clear();

    void registerEndpoint(ClientEndpoint endpoint);

    void removeEndpoint(ClientEndpoint endpoint);

    void removeEndpoint(ClientEndpoint endpoint, boolean closeImmediately);
}

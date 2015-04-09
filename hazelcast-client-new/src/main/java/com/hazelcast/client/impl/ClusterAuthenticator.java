package com.hazelcast.client.impl;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationCustomCredentialsParameters;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationParameters;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationResultParameters;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Used to authenticate client connections to cluster as parameter to ClientConnectionManager.
 *
 */
public class ClusterAuthenticator implements Authenticator {


    private final HazelcastClientInstanceImpl client;
    private final Credentials credentials;

    public ClusterAuthenticator(HazelcastClientInstanceImpl client, Credentials credentials) {
        this.client = client;
        this.credentials = credentials;
    }

    @Override
    public void authenticate(ClientConnection connection) throws AuthenticationException, IOException {
        final SerializationService ss = client.getSerializationService();
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        final ClientPrincipal principal = clusterService.getPrincipal();
        String uuid = principal.getUuid();
        String ownerUuid = principal.getOwnerUuid();

        ClientMessage clientMessage;
        if (credentials instanceof UsernamePasswordCredentials) {
            UsernamePasswordCredentials cr = (UsernamePasswordCredentials) credentials;
            clientMessage = AuthenticationParameters.encode(cr.getUsername(), cr.getPassword(), uuid, ownerUuid, false);
        } else {
            Data data = ss.toData(credentials);
            clientMessage = AuthenticationCustomCredentialsParameters.encode(data.toByteArray(), uuid, ownerUuid, false);

        }
        connection.init();

        ClientMessage response;
        final ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, connection);
        final Future<ClientMessage> future = clientInvocation.invoke();
        try {
            response = ss.toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e, IOException.class);
        }
        AuthenticationResultParameters resultParameters = AuthenticationResultParameters.decode(response);

        connection.setRemoteEndpoint(resultParameters.address);
    }
}

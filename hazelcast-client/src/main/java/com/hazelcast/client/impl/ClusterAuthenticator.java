package com.hazelcast.client.impl;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * Used to authenticate client connections to cluster as parameter to ClientConnectionManager.
 *
 * @see com.hazelcast.client.connection.ClientConnectionManager#getOrConnect(Address, Authenticator)
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
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        final ClientPrincipal principal = clusterService.getClusterListenerSupport().getPrincipal();
        final SerializationService ss = client.getSerializationService();
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        connection.init();
        //contains remoteAddress and principal
        SerializableCollection collectionWrapper;
        final ClientInvocation clientInvocation = new ClientInvocation(client, auth, connection);
        final Future<SerializableCollection> future = clientInvocation.invoke();
        try {
            collectionWrapper = ss.toObject(future.get());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e, IOException.class);
        }
        final Iterator<Data> iter = collectionWrapper.iterator();
        final Data addressData = iter.next();
        final Address address = ss.toObject(addressData);
        connection.setRemoteEndpoint(address);
    }
}

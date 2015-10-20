/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableList;
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
        final ClientPrincipal principal = clusterService.getPrincipal();
        final SerializationService ss = client.getSerializationService();
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        connection.init();
        //contains remoteAddress and principal
        SerializableList collectionWrapper;
        final ClientInvocation clientInvocation = new ClientInvocation(client, auth, connection);
        final Future<SerializableList> future = clientInvocation.invoke();
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

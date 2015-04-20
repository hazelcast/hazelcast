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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.LifecycleServiceImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationCustomCredentialsParameters;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationParameters;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationResultParameters;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;

public abstract class ClusterListenerSupport implements ConnectionListener, ConnectionHeartbeatListener,
        ClientClusterService {

    private static final ILogger LOGGER = Logger.getLogger(ClusterListenerSupport.class);

    protected final HazelcastClientInstanceImpl client;
    private final Collection<AddressProvider> addressProviders;
    private final ManagerAuthenticator managerAuthenticator = new ManagerAuthenticator();
    private final boolean shuffleMemberList;

    private Credentials credentials;
    private ClientConnectionManager connectionManager;
    private ClientListenerServiceImpl clientListenerService;
    private ClientMembershipListener clientMembershipListener;
    private volatile Address ownerConnectionAddress;
    private volatile ClientPrincipal principal;

    public ClusterListenerSupport(HazelcastClientInstanceImpl client) {
        this.client = client;

        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final ClientAwsConfig awsConfig = networkConfig.getAwsConfig();
        addressProviders = new LinkedList<AddressProvider>();

        addressProviders.add(new DefaultAddressProvider(networkConfig));

        if (awsConfig != null && awsConfig.isEnabled()) {
            try {
                addressProviders.add(new AwsAddressProvider(awsConfig));
            } catch (NoClassDefFoundError e) {
                LOGGER.log(Level.WARNING, "hazelcast-cloud.jar might be missing!");
                throw e;
            }
        }
        shuffleMemberList = client.getClientProperties().getShuffleMemberList().getBoolean();
    }

    protected void init() {
        this.connectionManager = client.getConnectionManager();
        this.clientListenerService = (ClientListenerServiceImpl) client.getListenerService();
        this.clientMembershipListener = new ClientMembershipListener(client);
        connectionManager.addConnectionListener(this);
        connectionManager.addConnectionHeartbeatListener(this);
        credentials = client.getCredentials();
    }

    public Address getOwnerConnectionAddress() {
        return ownerConnectionAddress;
    }

    private class ManagerAuthenticator implements Authenticator {

        @Override
        public void authenticate(ClientConnection connection) throws AuthenticationException, IOException {
            final SerializationService ss = client.getSerializationService();
            String uuid = "";
            String ownerUuid = "";
            if (principal != null) {
                uuid = principal.getUuid();
                ownerUuid = principal.getOwnerUuid();
            }

            ClientMessage clientMessage;
            if (credentials instanceof UsernamePasswordCredentials) {
                UsernamePasswordCredentials cr = (UsernamePasswordCredentials) credentials;
                clientMessage = AuthenticationParameters.encode(cr.getUsername(), cr.getPassword(), uuid, ownerUuid, true);
            } else {
                Data data = ss.toData(credentials);
                clientMessage = AuthenticationCustomCredentialsParameters.encode(data.toByteArray(), uuid, ownerUuid, true);

            }
            connection.init();

            ClientMessage response;
            final ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, connection);
            final Future<ClientMessage> future = clientInvocation.invoke();
            try {
                response = future.get();
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e, IOException.class);
            }
            AuthenticationResultParameters resultParameters = AuthenticationResultParameters.decode(response);

            connection.setRemoteEndpoint(resultParameters.address);

            principal = new ClientPrincipal(resultParameters.uuid, resultParameters.ownerUuid);
        }
    }

    protected void connectToCluster() throws Exception {
        connectToOne();
        clientMembershipListener.listenMembershipEvents(ownerConnectionAddress);
        clientListenerService.triggerFailedListeners();
    }

    private Collection<InetSocketAddress> getSocketAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();

        Collection<MemberImpl> memberList = getMemberList();
        for (MemberImpl member : memberList) {
            socketAddresses.add(member.getInetSocketAddress());
        }

        for (AddressProvider addressProvider : addressProviders) {
            socketAddresses.addAll(addressProvider.loadAddresses());
        }

        if (shuffleMemberList) {
            Collections.shuffle(socketAddresses);
        }

        return socketAddresses;
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    private void connectToOne() throws Exception {
        final ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final int connAttemptLimit = networkConfig.getConnectionAttemptLimit();
        final int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();

        final int connectionAttemptLimit = connAttemptLimit == 0 ? Integer.MAX_VALUE : connAttemptLimit;

        int attempt = 0;
        Set<InetSocketAddress> triedAddresses = new HashSet<InetSocketAddress>();
        while (attempt < connectionAttemptLimit) {
            if (!client.getLifecycleService().isRunning()) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Giving up on retrying to connect to cluster since client is shutdown");
                }
                break;
            }
            attempt++;
            final long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;

            boolean isConnected = connect(triedAddresses);

            if (isConnected) {
                return;
            }

            final long remainingTime = nextTry - Clock.currentTimeMillis();
            LOGGER.warning(
                    String.format("Unable to get alive cluster connection, try in %d ms later, attempt %d of %d.",
                            Math.max(0, remainingTime), attempt, connectionAttemptLimit));

            if (remainingTime > 0) {
                try {
                    Thread.sleep(remainingTime);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        throw new IllegalStateException("Unable to connect to any address in the config! "
                + "The following addresses were tried:" + triedAddresses);
    }

    private boolean connect(Set<InetSocketAddress> triedAddresses) throws Exception {
        final Collection<InetSocketAddress> socketAddresses = getSocketAddresses();
        for (InetSocketAddress inetSocketAddress : socketAddresses) {
            try {
                triedAddresses.add(inetSocketAddress);
                Address address = new Address(inetSocketAddress);
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Trying to connect to " + address);
                }
                final Connection connection = connectionManager.getOrConnect(address, managerAuthenticator);
                fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED);
                ownerConnectionAddress = connection.getEndPoint();
                return true;
            } catch (Exception e) {
                Level level = e instanceof AuthenticationException ? Level.WARNING : Level.FINEST;
                LOGGER.log(level, "Exception during initial connection to " + inetSocketAddress, e);
            }
        }
        return false;
    }

    private void fireConnectionEvent(LifecycleEvent.LifecycleState state) {
        final LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        lifecycleService.fireLifecycleEvent(state);
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            if (client.getLifecycleService().isRunning()) {
                executionService.executeInternal(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED);
                            connectToCluster();
                        } catch (Exception e) {
                            LOGGER.warning("Could not re-connect to cluster shutting down the client", e);
                            client.getLifecycleService().shutdown();
                        }
                    }
                });
            }
        }
    }

    @Override
    public void heartBeatStarted(Connection connection) {

    }

    @Override
    public void heartBeatStopped(Connection connection) {
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            connectionManager.destroyConnection(connection);
        }
    }
}


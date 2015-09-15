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
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.LifecycleServiceImpl;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class ClusterListenerSupport implements ConnectionListener, ConnectionHeartbeatListener,
        ClientClusterService {

    private static final ILogger LOGGER = Logger.getLogger(ClusterListenerSupport.class);
    private static final long TERMINATE_TIMEOUT_SECONDS = 30;

    protected final HazelcastClientInstanceImpl client;
    private final Collection<AddressProvider> addressProviders;
    private final ManagerAuthenticator managerAuthenticator = new ManagerAuthenticator();
    private final ExecutorService clusterExecutor;
    private final boolean shuffleMemberList;

    private Credentials credentials;
    private ClientConnectionManager connectionManager;
    private ClientListenerServiceImpl clientListenerService;
    private ClientMembershipListener clientMembershipListener;
    private volatile Address ownerConnectionAddress;
    private volatile ClientPrincipal principal;

    public ClusterListenerSupport(HazelcastClientInstanceImpl client, Collection<AddressProvider> addressProviders) {
        this.client = client;
        this.addressProviders = addressProviders;
        this.shuffleMemberList = client.getClientProperties().getShuffleMemberList().getBoolean();
        this.clusterExecutor = createSingleThreadExecutorService(client);
    }

    private ExecutorService createSingleThreadExecutorService(HazelcastClientInstanceImpl client) {
        ThreadGroup threadGroup = client.getThreadGroup();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        PoolExecutorThreadFactory threadFactory =
                new PoolExecutorThreadFactory(threadGroup, client.getName() + ".cluster-", classLoader);
        return Executors.newSingleThreadExecutor(threadFactory);
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

    public void shutdown() {
        clusterExecutor.shutdown();
        try {
            boolean success = clusterExecutor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                LOGGER.warning("ClientClusterService shutdown could not completed in "
                        + TERMINATE_TIMEOUT_SECONDS + " seconds");
            }
        } catch (InterruptedException e) {
            LOGGER.warning("ClientClusterService shutdown is interrupted", e);
        }
    }

    private class ManagerAuthenticator implements Authenticator {

        @Override
        public void authenticate(ClientConnection connection) throws AuthenticationException, IOException {
            final SerializationService ss = client.getSerializationService();
            AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
            connection.init();
            auth.setOwnerConnection(true);
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
            final Data principalData = iter.next();
            principal = ss.toObject(principalData);
        }
    }

    protected void connectToCluster() throws Exception {
        connectToOne();
        clientMembershipListener.listenMembershipEvents(ownerConnectionAddress);
        clientListenerService.triggerFailedListeners();
    }

    private Collection<InetSocketAddress> getSocketAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();

        Collection<Member> memberList = getMemberList();
        for (Member member : memberList) {
            socketAddresses.add(member.getSocketAddress());
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
        ownerConnectionAddress = null;
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

    private void fireConnectionEvent(final LifecycleEvent.LifecycleState state) {
        ClientExecutionService executionService = client.getClientExecutionService();
        executionService.execute(new Runnable() {
            @Override
            public void run() {
                final LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
                lifecycleService.fireLifecycleEvent(state);
            }
        });
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            if (client.getLifecycleService().isRunning()) {
                clusterExecutor.execute(new Runnable() {
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


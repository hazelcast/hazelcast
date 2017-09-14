/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.LifecycleServiceImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.spi.properties.ClientProperty.SHUFFLE_MEMBER_LIST;

public abstract class ClusterListenerSupport implements ConnectionListener, ConnectionHeartbeatListener, ClientClusterService {

    public static final long TERMINATE_TIMEOUT_SECONDS = 30;

    protected final HazelcastClientInstanceImpl client;

    private final Collection<AddressProvider> addressProviders;
    private final ExecutorService clusterExecutor;
    private final boolean shuffleMemberList;
    private final ILogger logger;

    private ClientConnectionManager connectionManager;
    private ClientMembershipListener clientMembershipListener;

    private volatile Address ownerConnectionAddress;
    private volatile ClientPrincipal principal;

    public ClusterListenerSupport(HazelcastClientInstanceImpl client, Collection<AddressProvider> addressProviders) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClusterListenerSupport.class);
        this.addressProviders = addressProviders;
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.clusterExecutor = createSingleThreadExecutorService(client);
    }

    private ExecutorService createSingleThreadExecutorService(HazelcastClientInstanceImpl client) {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        SingleExecutorThreadFactory threadFactory =
                new SingleExecutorThreadFactory(classLoader, client.getName() + ".cluster-");

        return Executors.newSingleThreadExecutor(threadFactory);
    }

    protected void init() {
        this.connectionManager = client.getConnectionManager();
        this.clientMembershipListener = new ClientMembershipListener(client);
        connectionManager.addConnectionListener(this);
        connectionManager.addConnectionHeartbeatListener(this);
    }

    @Override
    public Address getOwnerConnectionAddress() {
        return ownerConnectionAddress;
    }

    public void setOwnerConnectionAddress(Address ownerConnectionAddress) {
        this.ownerConnectionAddress = ownerConnectionAddress;
    }

    public void shutdown() {
        clusterExecutor.shutdown();
        try {
            boolean success = clusterExecutor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                logger.warning("cluster executor awaitTermination could not completed in "
                        + TERMINATE_TIMEOUT_SECONDS + " seconds");
            }
        } catch (InterruptedException e) {
            logger.warning("cluster executor await termination is interrupted", e);
        }
    }

    private Collection<Address> getAddresses() {
        final List<Address> addresses = new LinkedList<Address>();

        Collection<Member> memberList = getMemberList();
        for (Member member : memberList) {
            addresses.add(member.getAddress());
        }

        for (AddressProvider addressProvider : addressProviders) {
            addresses.addAll(addressProvider.loadAddresses());
        }

        if (shuffleMemberList) {
            Collections.shuffle(addresses);
        }

        return addresses;
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    public void setPrincipal(ClientPrincipal principal) {
        this.principal = principal;
    }

    public void connectToCluster() throws Exception {
        ownerConnectionAddress = null;

        final ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final int connAttemptLimit = networkConfig.getConnectionAttemptLimit();
        final int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();

        final int connectionAttemptLimit = connAttemptLimit == 0 ? Integer.MAX_VALUE : connAttemptLimit;

        int attempt = 0;
        Set<Address> triedAddresses = new HashSet<Address>();
        while (attempt < connectionAttemptLimit) {
            checkIfClientStillRuning("Giving up on retrying to connect to cluster since client is shutdown.");
            attempt++;
            final long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;

            boolean isConnected = connect(triedAddresses);

            if (isConnected) {
                return;
            }

            /**
             * If the address providers load no addresses (which seems to be possible), then the above loop is not entered
             * and the lifecycle check is missing, hence we need to repeat the same check at this point.
             */
            checkIfClientStillRuning("Client is being shutdown.");

            final long remainingTime = nextTry - Clock.currentTimeMillis();
            logger.warning(
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
        throw new IllegalStateException("Unable to connect to any address in the config!"
                + " The following addresses were tried: " + triedAddresses);
    }

    private void checkIfClientStillRuning(String shutdownMessage) {
        if (!client.getLifecycleService().isRunning()) {
            throw new IllegalStateException(shutdownMessage);
        }
    }

    private boolean connect(Set<Address> triedAddresses) throws Exception {
        final Collection<Address> memberAddresses = getAddresses();
        for (Address address : memberAddresses) {
            if (!client.getLifecycleService().isRunning()) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Giving up on retrying to connect to cluster since client is shutdown");
                }
                break;
            }
            Connection connection = null;
            try {
                triedAddresses.add(address);
                logger.info("Trying to connect to " + address + " as owner member");
                connection = connectionManager.getOrConnect(address, true);
                clientMembershipListener.listenMembershipEvents(ownerConnectionAddress);
                fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED);
                return true;
            } catch (Exception e) {
                logger.warning("Exception during initial connection to " + address + ", exception " + e);
                if (null != connection) {
                    connection.close("Could not connect to " + address + " as owner", e);
                }
            }
        }
        return false;
    }

    private void fireConnectionEvent(final LifecycleEvent.LifecycleState state) {
        final LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        lifecycleService.fireLifecycleEvent(state);
    }

    @Override
    public void connectionAdded(Connection connection) {
    }

    @Override
    public void connectionRemoved(Connection connection) {
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            if (client.getLifecycleService().isRunning()) {
                fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED);

                clusterExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            connectToCluster();
                        } catch (Exception e) {
                            logger.warning("Could not re-connect to cluster shutting down the client" + e.getMessage());
                            new Thread(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        client.getLifecycleService().shutdown();
                                    } catch (Exception exception) {
                                        logger.severe("Exception during client shutdown ", exception);
                                    }
                                }
                            }, client.getName() + ".clientShutdown-").start();
                        }
                    }
                });
            }
        }
    }

    @Override
    public void heartbeatResumed(Connection connection) {
    }

    @Override
    public void heartbeatStopped(Connection connection) {
        if (connection.getEndPoint().equals(ownerConnectionAddress)) {
            connection.close(null,
                    new TargetDisconnectedException("Heartbeat timed out to owner connection " + connection));
        }
    }
}

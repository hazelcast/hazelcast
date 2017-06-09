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

package com.hazelcast.client.connection;

import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.net.InetSocketAddress;
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

/**
 *
 */
public abstract class ClientConnectionStrategy {

    /**
     *
     */
    public static final long TERMINATE_TIMEOUT_SECONDS = 30;

    protected HazelcastClientInstanceImpl client;
    protected ClientConnectionManagerImpl connectionManager;
    protected ILogger logger;
    protected boolean shuffleMemberList;
    protected ExecutorService clusterConnectionExecutor;
    protected Collection<AddressProvider> addressProviders;

    protected int connectionAttemptPeriod;
    protected int connectionAttemptLimit;
    protected boolean clientStartAsync;
    protected ClientConnectionStrategyConfig.ReconnectMode reconnectMode;

    public ClientConnectionStrategy() {
    }

    public final void init(HazelcastClientInstanceImpl client,
                           ClientConnectionManagerImpl clientConnectionManager,
                           Collection<AddressProvider> addressProviders) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientConnectionStrategy.class);
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.addressProviders = addressProviders;
        this.clusterConnectionExecutor = createSingleThreadExecutorService(client);
        this.connectionManager = clientConnectionManager;

        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        int connAttemptLimit = networkConfig.getConnectionAttemptLimit();
        connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();
        connectionAttemptLimit = connAttemptLimit == 0 ? Integer.MAX_VALUE : connAttemptLimit;

        ClientConnectionStrategyConfig config = client.getClientConfig().getConnectionStrategyConfig();
        clientStartAsync = config.isAsyncStart();
        reconnectMode = config.getReconnectMode();
    }

    protected void connectToCluster() {
        connectionManager.setOwnerConnectionAddress(null);

        int attempt = 0;
        Set<InetSocketAddress> triedAddresses = new HashSet<InetSocketAddress>();
        while (attempt < connectionAttemptLimit) {
            attempt++;
            final long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;

            final Collection<InetSocketAddress> socketAddresses = getSocketAddresses();
            for (InetSocketAddress inetSocketAddress : socketAddresses) {
                if (!client.getLifecycleService().isRunning()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Giving up on retrying to connect to cluster since client is shutdown.");
                    }
                    break;
                }
                triedAddresses.add(inetSocketAddress);
                if (connectionManager.connectAsOwner(inetSocketAddress) != null) {
                    return;
                }
            }

            final long remainingTime = nextTry - Clock.currentTimeMillis();
            logger.warning(String.format("Unable to get alive cluster connection, try in %d ms later, attempt %d of %d.",
                    Math.max(0, remainingTime), attempt, connectionAttemptLimit));

            if (remainingTime > 0) {
                try {
                    Thread.sleep(remainingTime);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        throw new IllegalStateException(
                "Unable to connect to any address in the config!" + " The following addresses were tried: " + triedAddresses);
    }

    protected void connectToClusterAsync() {
        clusterConnectionExecutor.execute(new Runnable() {
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

    private Collection<InetSocketAddress> getSocketAddresses() {
        final List<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();

        Collection<Member> memberList = client.getClientClusterService().getMemberList();
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

    private ExecutorService createSingleThreadExecutorService(HazelcastClientInstanceImpl client) {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        SingleExecutorThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, client.getName() + ".cluster-");

        return Executors.newSingleThreadExecutor(threadFactory);
    }

    /**
     */
    public abstract void init();

    /**
     * @param target
     */
    public abstract void beforeGetConnection(Address target);

    /**
     * @param target
     */
    public abstract void beforeOpenConnection(Address target);

    /**
     */
    public abstract void onConnectToCluster();

    /**
     *
     */
    public abstract void onDisconnectFromCluster();

    /**
     * @param connection
     */
    public abstract void onConnect(ClientConnection connection);

    /**
     * @param connection
     */
    public abstract void onDisconnect(ClientConnection connection);

    /**
     * @param connection
     */
    public abstract void onHeartbeatStopped(ClientConnection connection);

    /**
     * @param connection
     */
    public abstract void onHeartbeatResumed(ClientConnection connection);

    /**
     *
     */
    public void shutdown() {
        clusterConnectionExecutor.shutdown();
        try {
            boolean success = clusterConnectionExecutor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                logger.warning(
                        "cluster executor awaitTermination could not completed in " + TERMINATE_TIMEOUT_SECONDS + " seconds");
            }
        } catch (InterruptedException e) {
            logger.warning("cluster executor await termination is interrupted", e);
        }
    }
}

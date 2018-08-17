/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.ClientConnectionStrategy;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.LifecycleServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.client.spi.properties.ClientProperty.SHUFFLE_MEMBER_LIST;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Helper to ClientConnectionManager.
 * selecting owner connection, connecting and disconnecting from cluster implemented in this class.
 */
class ClusterConnector {

    private static final int DEFAULT_CONNECTION_ATTEMPT_LIMIT_SYNC = 2;
    private static final int DEFAULT_CONNECTION_ATTEMPT_LIMIT_ASYNC = 20;

    private final ILogger logger;
    private final HazelcastClientInstanceImpl client;
    private final ClientConnectionManagerImpl connectionManager;
    private final ClientConnectionStrategy connectionStrategy;
    private final ExecutorService clusterConnectionExecutor;
    private final boolean shuffleMemberList;
    private final WaitStrategy waitStrategy;
    private final Collection<AddressProvider> addressProviders;
    private volatile Address ownerConnectionAddress;
    private volatile Address previousOwnerConnectionAddress;

    ClusterConnector(HazelcastClientInstanceImpl client,
                     ClientConnectionManagerImpl connectionManager,
                     ClientConnectionStrategy connectionStrategy,
                     Collection<AddressProvider> addressProviders) {
        this.client = client;
        this.connectionManager = connectionManager;
        this.logger = client.getLoggingService().getLogger(ClientConnectionManager.class);
        this.connectionStrategy = connectionStrategy;
        this.clusterConnectionExecutor = createSingleThreadExecutorService(client);
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.addressProviders = addressProviders;
        this.waitStrategy = initializeWaitStrategy(client.getClientConfig());
    }

    private WaitStrategy initializeWaitStrategy(ClientConfig clientConfig) {
        ClientConnectionStrategyConfig connectionStrategyConfig = client.getClientConfig().getConnectionStrategyConfig();
        ConnectionRetryConfig expoRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        if (expoRetryConfig.isEnabled()) {
            return new ExponentialWaitStrategy(expoRetryConfig.getInitialBackoffMillis(),
                    expoRetryConfig.getMaxBackoffMillis(),
                    expoRetryConfig.getMultiplier(),
                    expoRetryConfig.isFailOnMaxBackoff(),
                    expoRetryConfig.getJitter());
        }
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();

        boolean isAsync = connectionStrategyConfig.isAsyncStart();

        int connectionAttemptLimit = networkConfig.getConnectionAttemptLimit();
        if (connectionAttemptLimit < 0) {
            connectionAttemptLimit = isAsync ? DEFAULT_CONNECTION_ATTEMPT_LIMIT_ASYNC
                    : DEFAULT_CONNECTION_ATTEMPT_LIMIT_SYNC;
        } else {
            connectionAttemptLimit = connectionAttemptLimit == 0 ? Integer.MAX_VALUE : connectionAttemptLimit;
        }

        return new DefaultWaitStrategy(connectionAttemptPeriod, connectionAttemptLimit);

    }

    void connectToCluster() {
        try {
            connectToClusterAsync().get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }


    Address getOwnerConnectionAddress() {
        return ownerConnectionAddress;
    }

    void setOwnerConnectionAddress(Address ownerConnectionAddress) {
        this.previousOwnerConnectionAddress = this.ownerConnectionAddress;
        this.ownerConnectionAddress = ownerConnectionAddress;
    }

    Connection connectAsOwner(Address address) {
        Connection connection = null;
        try {
            logger.info("Trying to connect to " + address + " as owner member");
            connection = connectionManager.getOrConnect(address, true);
            client.onClusterConnect(connection);
            fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED);
            connectionStrategy.onConnectToCluster();
        } catch (Exception e) {
            logger.warning("Exception during initial connection to " + address + ", exception " + e);
            if (null != connection) {
                connection.close("Could not connect to " + address + " as owner", e);
            }
            return null;
        }
        return connection;
    }

    void disconnectFromCluster(final ClientConnection connection) {
        clusterConnectionExecutor.execute(new Runnable() {
            @Override
            public void run() {
                Address endpoint = connection.getEndPoint();
                // it may be possible that while waiting on executor queue, the client got connected (another connection),
                // then we do not need to do anything for cluster disconnect.
                if (endpoint == null || !endpoint.equals(ownerConnectionAddress)) {
                    return;
                }

                setOwnerConnectionAddress(null);
                connectionStrategy.onDisconnectFromCluster();

                if (client.getLifecycleService().isRunning()) {
                    fireConnectionEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED);
                }
            }
        });
    }

    private void fireConnectionEvent(final LifecycleEvent.LifecycleState state) {
        final LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        lifecycleService.fireLifecycleEvent(state);
    }

    private ExecutorService createSingleThreadExecutorService(HazelcastClientInstanceImpl client) {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        SingleExecutorThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, client.getName() + ".cluster-");

        return Executors.newSingleThreadExecutor(threadFactory);
    }

    private void connectToClusterInternal() {
        Set<Address> triedAddresses = new HashSet<Address>();

        waitStrategy.reset();
        do {
            Collection<Address> addresses = getPossibleMemberAddresses();
            for (Address address : addresses) {
                if (!client.getLifecycleService().isRunning()) {
                    throw new IllegalStateException("Giving up on retrying to connect to cluster since client is shutdown.");
                }
                triedAddresses.add(address);
                if (connectAsOwner(address) != null) {
                    return;
                }
            }

            // If the address providers load no addresses (which seems to be possible), then the above loop is not entered
            // and the lifecycle check is missing, hence we need to repeat the same check at this point.
            if (!client.getLifecycleService().isRunning()) {
                throw new IllegalStateException("Client is being shutdown.");
            }

        } while (waitStrategy.sleep());
        throw new IllegalStateException(
                "Unable to connect to any address! The following addresses were tried: " + triedAddresses);
    }

    Future<Void> connectToClusterAsync() {
        return clusterConnectionExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    connectToClusterInternal();
                } catch (Exception e) {
                    logger.warning("Could not connect to cluster, shutting down the client. " + e.getMessage());
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

                    throw rethrow(e);
                }
                return null;
            }
        });

    }

    Collection<Address> getPossibleMemberAddresses() {
        LinkedHashSet<Address> addresses = new LinkedHashSet<Address>();

        Collection<Member> memberList = client.getClientClusterService().getMemberList();
        for (Member member : memberList) {
            addresses.add(member.getAddress());
        }

        if (shuffleMemberList) {
            addresses = (LinkedHashSet<Address>) shuffle(addresses);
        }

        LinkedHashSet<Address> providerAddresses = new LinkedHashSet<Address>();
        for (AddressProvider addressProvider : addressProviders) {
            try {
                providerAddresses.addAll(addressProvider.loadAddresses());
            } catch (NullPointerException e) {
                throw e;
            } catch (Exception e) {
                logger.warning("Exception from AddressProvider: " + addressProvider, e);
            }
        }

        if (shuffleMemberList) {
            providerAddresses = (LinkedHashSet<Address>) shuffle(providerAddresses);
        }

        addresses.addAll(providerAddresses);

        if (previousOwnerConnectionAddress != null) {
            /*
             * Previous owner address is moved to last item in set so that client will not try to connect to same one immediately.
             * It could be the case that address is removed because it is healthy(it not responding to heartbeat/pings)
             * In that case, trying other addresses first to upgrade make more sense.
             */
            addresses.remove(previousOwnerConnectionAddress);
            addresses.add(previousOwnerConnectionAddress);
        }
        return addresses;
    }

    private static <T> Set<T> shuffle(Set<T> set) {
        List<T> shuffleMe = new ArrayList<T>(set);
        Collections.shuffle(shuffleMe);
        return new LinkedHashSet<T>(shuffleMe);
    }

    public void shutdown() {
        ClientExecutionServiceImpl.shutdownExecutor("cluster", clusterConnectionExecutor, logger);
    }

    interface WaitStrategy {

        void reset();

        boolean sleep();

    }

    class DefaultWaitStrategy implements WaitStrategy {

        private final int connectionAttemptPeriod;
        private final int connectionAttemptLimit;
        private int attempt;

        DefaultWaitStrategy(int connectionAttemptPeriod, int connectionAttemptLimit) {
            this.connectionAttemptPeriod = connectionAttemptPeriod;
            this.connectionAttemptLimit = connectionAttemptLimit;
        }

        @Override
        public void reset() {
            attempt = 0;
        }

        @Override
        public boolean sleep() {
            attempt++;
            if (attempt >= connectionAttemptLimit) {
                logger.warning(String.format("Unable to get alive cluster connection, attempt %d of %d.", attempt,
                        connectionAttemptLimit));
                return false;
            }
            logger.warning(String.format("Unable to get alive cluster connection, try in %d ms later, attempt %d of %d.",
                    connectionAttemptPeriod, attempt, connectionAttemptLimit));

            try {
                Thread.sleep(connectionAttemptPeriod);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }

    }


    class ExponentialWaitStrategy implements WaitStrategy {

        private final int initialBackoffMillis;
        private final int maxBackoffMillis;
        private final double multiplier;
        private final boolean failOnMaxBackoff;
        private final double jitter;
        private final Random random = new Random();

        private int attempt;
        private int currentBackoffMillis;

        ExponentialWaitStrategy(int initialBackoffMillis, int maxBackoffMillis,
                                double multiplier, boolean failOnMaxBackoff, double jitter) {
            this.initialBackoffMillis = initialBackoffMillis;
            this.maxBackoffMillis = maxBackoffMillis;
            this.multiplier = multiplier;
            this.failOnMaxBackoff = failOnMaxBackoff;
            this.jitter = jitter;
        }

        @Override
        public void reset() {
            attempt = 0;
            currentBackoffMillis = Math.min(maxBackoffMillis, initialBackoffMillis);
        }

        @Override
        public boolean sleep() {
            attempt++;
            if (failOnMaxBackoff && currentBackoffMillis >= maxBackoffMillis) {
                logger.warning(String.format("Unable to get alive cluster connection, attempt %d.", attempt));
                return false;
            }
            //random_between
            // Random(-jitter * current_backoff, jitter * current_backoff)
            long actualSleepTime = (long) (currentBackoffMillis - (currentBackoffMillis * jitter)
                    + (currentBackoffMillis * jitter * random.nextDouble()));

            logger.warning(String.format("Unable to get alive cluster connection, try in %d ms later, attempt %d "
                    + ", cap retry timeout millis %d", actualSleepTime, attempt, maxBackoffMillis));

            try {
                Thread.sleep(actualSleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            currentBackoffMillis = (int) Math.min(currentBackoffMillis * multiplier, maxBackoffMillis);
            return true;
        }
    }
}

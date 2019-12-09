/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.nio;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientNotAllowedInClusterException;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.ClientDiscoveryService;
import com.hazelcast.client.impl.clientside.ClientLoggingService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.LifecycleServiceImpl;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.protocol.codec.ClientIsFailoverSupportedCodec;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.LoggingScheduledExecutor;
import com.hazelcast.internal.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.PasswordCredentials;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static com.hazelcast.client.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.client.properties.ClientProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_WRITE_THROUGH_ENABLED;
import static com.hazelcast.client.properties.ClientProperty.SHUFFLE_MEMBER_LIST;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final int DEFAULT_SMART_CLIENT_THREAD_COUNT = 3;
    private static final int EXECUTOR_CORE_POOL_SIZE = 10;
    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    private final AtomicBoolean isAlive = new AtomicBoolean();
    private final ILogger logger;
    private final int connectionTimeoutMillis;
    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<Address, InetSocketAddress> inetSocketAddressCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<InetSocketAddress, ClientConnection> activeConnections = new ConcurrentHashMap<>();
    private final Collection<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();
    private final NioNetworking networking;
    private final HeartbeatManager heartbeat;
    private final long authenticationTimeout;
    private final String connectionType;
    private final UUID clientUuid = UuidUtil.newUnsecureUUID();
    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<>();
    private final Set<String> labels;
    private final int outboundPortCount;
    private final boolean failoverConfigProvided;
    private final ScheduledExecutorService clusterConnectionExecutor;
    private final boolean shuffleMemberList;
    private final WaitStrategy waitStrategy;
    private final ClientDiscoveryService discoveryService;
    private final AtomicInteger connectionCount = new AtomicInteger();
    private final boolean asyncStart;
    private final ClientConnectionStrategyConfig.ReconnectMode reconnectMode;
    private final LoadBalancer loadBalancer;
    private final boolean isSmartRoutingEnabled;
    private volatile Credentials currentCredentials;
    private volatile int partitionCount = -1;
    private volatile UUID clusterId;
    private volatile AtomicReference<ClusterState> state = new AtomicReference<>(ClusterState.STARTING);

    private enum ClusterState {
        STARTING, CONNECTED, DISCONNECTED
    }

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.loadBalancer = client.getLoadBalancer();
        this.labels = Collections.unmodifiableSet(client.getClientConfig().getLabels());
        this.logger = client.getLoggingService().getLogger(ClientConnectionManager.class);
        this.connectionType = client.getProperties().getBoolean(MC_CLIENT_MODE_PROP)
                ? ConnectionType.MC_JAVA_CLIENT : ConnectionType.JAVA_CLIENT;
        this.connectionTimeoutMillis = initConnectionTimeoutMillis();
        this.networking = initNetworking();
        this.outboundPorts.addAll(getOutboundPorts());
        this.outboundPortCount = outboundPorts.size();
        this.heartbeat = new HeartbeatManager(this, client);
        this.authenticationTimeout = heartbeat.getHeartbeatTimeout();
        this.failoverConfigProvided = client.getFailoverConfig() != null;
        this.clusterConnectionExecutor = createExecutorService();
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.discoveryService = client.getClientDiscoveryService();
        this.isSmartRoutingEnabled = client.getClientConfig().getNetworkConfig().isSmartRouting();
        this.waitStrategy = initializeWaitStrategy(client.getClientConfig());
        ClientConnectionStrategyConfig connectionStrategyConfig = client.getClientConfig().getConnectionStrategyConfig();
        this.asyncStart = connectionStrategyConfig.isAsyncStart();
        this.reconnectMode = connectionStrategyConfig.getReconnectMode();
    }

    private int initConnectionTimeoutMillis() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
    }

    private ScheduledExecutorService createExecutorService() {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        String name = client.getName();
        return new LoggingScheduledExecutor(logger, EXECUTOR_CORE_POOL_SIZE,
                new PoolExecutorThreadFactory(name + ".internal-", classLoader), (r, executor) -> {
            String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
            logger.finest(message);
            throw new RejectedExecutionException(message);
        });
    }

    private Collection<Integer> getOutboundPorts() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        Collection<Integer> outboundPorts = networkConfig.getOutboundPorts();
        Collection<String> outboundPortDefinitions = networkConfig.getOutboundPortDefinitions();
        return AddressUtil.getOutboundPorts(outboundPorts, outboundPortDefinitions);
    }

    public NioNetworking getNetworking() {
        return networking;
    }

    protected NioNetworking initNetworking() {
        HazelcastProperties properties = client.getProperties();

        int configuredInputThreads = properties.getInteger(IO_INPUT_THREAD_COUNT);
        int configuredOutputThreads = properties.getInteger(IO_OUTPUT_THREAD_COUNT);

        int inputThreads;
        if (configuredInputThreads == -1) {
            inputThreads = isSmartRoutingEnabled ? DEFAULT_SMART_CLIENT_THREAD_COUNT : 1;
        } else {
            inputThreads = configuredInputThreads;
        }

        int outputThreads;
        if (configuredOutputThreads == -1) {
            outputThreads = isSmartRoutingEnabled ? DEFAULT_SMART_CLIENT_THREAD_COUNT : 1;
        } else {
            outputThreads = configuredOutputThreads;
        }

        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(client.getLoggingService())
                        .metricsRegistry(client.getMetricsRegistry())
                        .threadNamePrefix(client.getName())
                        .errorHandler(new ClientConnectionChannelErrorHandler())
                        .inputThreadCount(inputThreads)
                        .outputThreadCount(outputThreads)
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .writeThroughEnabled(properties.getBoolean(IO_WRITE_THROUGH_ENABLED))
                        .concurrencyDetection(client.getConcurrencyDetection()));
    }

    private WaitStrategy initializeWaitStrategy(ClientConfig clientConfig) {
        ClientConnectionStrategyConfig connectionStrategyConfig = clientConfig.getConnectionStrategyConfig();
        ConnectionRetryConfig expoRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        return new WaitStrategy(expoRetryConfig.getInitialBackoffMillis(),
                expoRetryConfig.getMaxBackoffMillis(),
                expoRetryConfig.getMultiplier(),
                expoRetryConfig.getClusterConnectTimeoutMillis(),
                expoRetryConfig.getJitter(), logger);
    }

    public synchronized void start() {
        if (!isAlive.compareAndSet(false, true)) {
            return;
        }
        startNetworking();

        heartbeat.start();
        connectToCluster();
        if (isSmartRoutingEnabled) {
            clusterConnectionExecutor.scheduleWithFixedDelay(this::tryOpenConnectionToAllMembers, 1, 1, TimeUnit.SECONDS);
        }
    }

    public void tryOpenConnectionToAllMembers() {
        if (!isSmartRoutingEnabled) {
            return;
        }
        Collection<Member> memberList = client.getClientClusterService().getMemberList();
        for (Member member : memberList) {
            if (!client.getLifecycleService().isRunning()) {
                return;
            }
            Address address = member.getAddress();
            if (getConnection(address) != null) {
                continue;
            }
            try {
                getOrConnect(address);
            } catch (Exception e) {
                EmptyStatement.ignore(e);
            }
        }
    }

    protected void startNetworking() {
        networking.restart();
    }

    public synchronized void shutdown() {
        if (!isAlive.compareAndSet(true, false)) {
            return;
        }

        ClientExecutionServiceImpl.shutdownExecutor("cluster", clusterConnectionExecutor, logger);
        for (Connection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }

        stopNetworking();
        connectionListeners.clear();
        heartbeat.shutdown();
        discoveryService.current().destroy();
    }

    protected void stopNetworking() {
        networking.shutdown();
    }

    private void connectToCluster() {
        CandidateClusterContext currentClusterContext = discoveryService.current();
        currentClusterContext.start();

        if (asyncStart) {
            connectToClusterAsync();
        } else {
            connectToClusterSync();
        }
    }

    private void connectToClusterAsync() {
        clusterConnectionExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    connectToClusterSync();
                } catch (Throwable e) {
                    logger.warning("Could not connect to any cluster, shutting down the client: " + e.getMessage());
                    shutdownWithExternalThread();
                }
            }
        });
    }

    private void connectToClusterSync() {
        CandidateClusterContext currentClusterContext = discoveryService.current();
        logger.info("Trying to connect to cluster with client name: " + currentClusterContext.getClusterName());
        if (connectToCandidate(currentClusterContext)) {
            return;
        }

        // we reset the search so that we will iterate the list try-count times, each time we start searching for a new cluster
        discoveryService.resetSearch();

        while (discoveryService.hasNext() && client.getLifecycleService().isRunning()) {
            discoveryService.current().destroy();
            client.onClusterChange();
            CandidateClusterContext candidateClusterContext = discoveryService.next();
            candidateClusterContext.start();
            ((ClientLoggingService) client.getLoggingService()).updateClusterName(candidateClusterContext.getClusterName());

            logger.info("Trying to connect to next cluster with client name: " + candidateClusterContext.getClusterName());

            if (connectToCandidate(candidateClusterContext)) {
                client.waitForInitialMembershipEvents();
                fireLifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER);
                return;
            }
        }
        if (!client.getLifecycleService().isRunning()) {
            throw new IllegalStateException("Client is being shutdown.");
        } else {
            throw new IllegalStateException("Unable to connect to any cluster.");
        }
    }

    private Connection connect(Address address) {
        try {
            logger.info("Trying to connect to " + address);
            return getOrConnect(address);
        } catch (InvalidConfigurationException e) {
            logger.warning("Exception during initial connection to " + address + ": " + e);
            throw rethrow(e);
        } catch (ClientNotAllowedInClusterException e) {
            logger.warning("Exception during initial connection to " + address + ": " + e);
            throw e;
        } catch (Exception e) {
            logger.warning("Exception during initial connection to " + address + ": " + e);
            return null;
        }
    }

    private void fireLifecycleEvent(LifecycleEvent.LifecycleState state) {
        LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        lifecycleService.fireLifecycleEvent(state);
    }

    private boolean connectToCandidate(CandidateClusterContext context) {
        Set<Address> triedAddresses = new HashSet<Address>();
        try {
            waitStrategy.reset();
            do {
                Collection<Address> addresses = getPossibleMemberAddresses(context.getAddressProvider());
                for (Address address : addresses) {
                    checkClientActive();
                    triedAddresses.add(address);

                    Connection connection = connect(address);
                    if (connection != null) {
                        return checkFailoverSupport(connection);
                    }
                }

                // If the address providers load no addresses (which seems to be possible), then the above loop is not entered
                // and the lifecycle check is missing, hence we need to repeat the same check at this point.
                checkClientActive();

            } while (waitStrategy.sleep());
        } catch (ClientNotAllowedInClusterException | InvalidConfigurationException e) {
            logger.warning("Give up trying on this cluster with name: " + context.getClusterName()
                    + " reason: " + e.getMessage());
        }
        logger.info("Unable to connect to any address from the cluster with name: " + context.getClusterName()
                + ". The following addresses were tried: " + triedAddresses);
        return false;
    }

    @Override
    public void checkInvocationAllowed() throws IOException {
        ClusterState state = this.state.get();
        if (state.equals(ClusterState.CONNECTED)) {
            return;
        }

        if (state.equals(ClusterState.STARTING)) {
            if (asyncStart) {
                throw new HazelcastClientOfflineException();
            } else {
                throw new IOException("No connection found to cluster since the client is starting.");
            }
        }

        if (ClientConnectionStrategyConfig.ReconnectMode.ASYNC.equals(reconnectMode)) {
            throw new HazelcastClientOfflineException();
        } else {
            throw new IOException("No connection found to cluster.");
        }
    }

    Collection<Address> getPossibleMemberAddresses(AddressProvider addressProvider) {
        LinkedHashSet<Address> addresses = new LinkedHashSet<>();

        Collection<Member> memberList = client.getClientClusterService().getMemberList();
        for (Member member : memberList) {
            addresses.add(member.getAddress());
        }

        if (shuffleMemberList) {
            addresses = (LinkedHashSet<Address>) shuffle(addresses);
        }

        LinkedHashSet<Address> providedAddresses = new LinkedHashSet<Address>();
        try {
            Addresses result = addressProvider.loadAddresses();
            if (shuffleMemberList) {
                // The relative order between primary and secondary addresses should not be changed.
                // so we shuffle the lists separately and then add them to the final list so that
                // secondary addresses are not tried before all primary addresses have been tried.
                // Otherwise we can get startup delays.
                Collections.shuffle(result.primary());
                Collections.shuffle(result.secondary());
            }
            providedAddresses.addAll(result.primary());
            providedAddresses.addAll(result.secondary());
        } catch (NullPointerException e) {
            throw e;
        } catch (Exception e) {
            logger.warning("Exception from AddressProvider: " + discoveryService, e);
        }

        addresses.addAll(providedAddresses);

        return addresses;
    }

    private static <T> Set<T> shuffle(Set<T> set) {
        List<T> shuffleMe = new ArrayList<T>(set);
        Collections.shuffle(shuffleMe);
        return new LinkedHashSet<T>(shuffleMe);
    }

    private void triggerReconnectToCluster() {
        if (reconnectMode == OFF) {
            logger.info("RECONNECT MODE is off. Shutting down the client");
            shutdownWithExternalThread();
            return;
        }
        if (client.getLifecycleService().isRunning()) {
            try {
                connectToClusterAsync();
            } catch (RejectedExecutionException r) {
                shutdownWithExternalThread();
            }
        }
    }

    private void shutdownWithExternalThread() {
        new Thread(() -> {
            try {
                client.getLifecycleService().shutdown();
            } catch (Exception exception) {
                logger.severe("Exception during client shutdown ", exception);
            }
        }, client.getName() + ".clientShutdown-").start();
    }

    @Override
    public Collection<ClientConnection> getActiveConnections() {
        return activeConnections.values();
    }

    @Override
    public boolean isAlive() {
        return isAlive.get();
    }

    @Override
    public UUID getClientUuid() {
        return clientUuid;
    }

    @Override
    public Connection getConnection(@Nonnull Address target) {
        return activeConnections.get(resolveAddress(target));
    }

    Connection getOrConnect(@Nonnull Address address) {
        checkClientActive();
        InetSocketAddress inetSocketAddress = resolveAddress(address);
        ClientConnection connection = activeConnections.get(inetSocketAddress);
        if (connection != null) {
            return connection;
        }
        synchronized (inetSocketAddress) {
            connection = activeConnections.get(inetSocketAddress);
            if (connection != null) {
                return connection;
            }

            address = translate(address);
            connection = createSocketConnection(address);
            initializeToCluster(connection);
            return connection;
        }
    }

    private void fireConnectionAddedEvent(ClientConnection connection) {
        for (ConnectionListener connectionListener : connectionListeners) {
            connectionListener.connectionAdded(connection);
        }
    }

    private void fireConnectionRemovedEvent(ClientConnection connection) {
        for (ConnectionListener listener : connectionListeners) {
            listener.connectionRemoved(connection);
        }
    }

    private boolean useAnyOutboundPort() {
        return outboundPortCount == 0;
    }

    private int acquireOutboundPort() {
        if (outboundPortCount == 0) {
            return 0;
        }
        synchronized (outboundPorts) {
            final Integer port = outboundPorts.removeFirst();
            outboundPorts.addLast(port);
            return port;
        }
    }

    private void bindSocketToPort(Socket socket) throws IOException {
        if (useAnyOutboundPort()) {
            SocketAddress socketAddress = new InetSocketAddress(0);
            socket.bind(socketAddress);
        } else {
            int retryCount = outboundPortCount * 2;
            IOException ex = null;
            for (int i = 0; i < retryCount; i++) {
                int port = acquireOutboundPort();
                if (port == 0) {
                    // fast-path for ephemeral range - no need to bind
                    return;
                }
                SocketAddress socketAddress = new InetSocketAddress(port);
                try {
                    socket.bind(socketAddress);
                    return;
                } catch (IOException e) {
                    ex = e;
                    logger.finest("Could not bind port[ " + port + "]: " + e.getMessage());
                }
            }
            throw ex;
        }
    }

    @SuppressWarnings("unchecked")
    protected ClientConnection createSocketConnection(Address target) {
        CandidateClusterContext currentClusterContext = discoveryService.current();
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();

            bindSocketToPort(socket);

            ChannelInitializerProvider channelInitializer = currentClusterContext.getChannelInitializerProvider();
            Channel channel = networking.register(null, channelInitializer, socketChannel, true);
            channel.attributeMap().put(Address.class, target);
            channel.connect(resolveAddress(target), connectionTimeoutMillis);

            ClientConnection connection = new ClientConnection(client, connectionIdGen.incrementAndGet(), channel);

            socketChannel.configureBlocking(true);
            SocketInterceptor socketInterceptor = currentClusterContext.getSocketInterceptor();
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }

            channel.start();
            return connection;
        } catch (Exception e) {
            closeResource(socketChannel);
            logger.finest(e);
            throw rethrow(e);
        }
    }

    private Address translate(Address target) {
        CandidateClusterContext currentClusterContext = discoveryService.current();
        AddressProvider addressProvider = currentClusterContext.getAddressProvider();
        try {
            Address translatedAddress = addressProvider.translate(target);
            if (translatedAddress == null) {
                throw new NullPointerException("Address Provider " + addressProvider.getClass()
                        + " could not translate address " + target);
            }
            return translatedAddress;
        } catch (Exception e) {
            logger.warning("Failed to translate address " + target + " via address provider " + e.getMessage());
            throw rethrow(e);
        }
    }

    void onClose(Connection connection) {
        removeFromActiveConnections((ClientConnection) connection);
    }

    private void removeFromActiveConnections(ClientConnection connection) {
        Address endpoint = connection.getEndPoint();

        if (endpoint == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying " + connection + ", but it has end-point set to null "
                        + "-> not removing it from a connection map");
            }
            return;
        }
        if (activeConnections.remove(resolveAddress(endpoint), connection)) {
            logger.info("Removed connection to endpoint: " + endpoint + ", connection: " + connection);
            if (connectionCount.decrementAndGet() == 0) {
                if (state.compareAndSet(ClusterState.CONNECTED, ClusterState.DISCONNECTED)) {
                    fireLifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED);
                    triggerReconnectToCluster();
                }

            }
            fireConnectionRemovedEvent(connection);
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying a connection, but there is no mapping " + endpoint + " -> " + connection
                        + " in the connection map.");
            }
        }
    }

    @Override
    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    public Credentials getCurrentCredentials() {
        return currentCredentials;
    }

    public void reset() {
        for (ClientConnection activeConnection : activeConnections.values()) {
            activeConnection.close(null, new TargetDisconnectedException("Closing since client is switching cluster"));
        }

        inetSocketAddressCache.clear();
    }

    @Override
    public Connection getRandomConnection() {
        Connection connection = null;
        if (isSmartRoutingEnabled) {
            Member member = loadBalancer.next();
            if (member != null) {
                connection = getConnection(member.getAddress());
            }
        }
        if (connection != null) {
            return connection;
        }
        Iterator<ClientConnection> iterator = activeConnections.values().iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    private class ClientConnectionChannelErrorHandler implements ChannelErrorHandler {
        @Override
        public void onError(Channel channel, Throwable cause) {
            if (channel == null) {
                logger.severe(cause);
            } else {
                if (cause instanceof OutOfMemoryError) {
                    logger.severe(cause);
                }

                Connection connection = (Connection) channel.attributeMap().get(ClientConnection.class);
                if (cause instanceof EOFException) {
                    connection.close("Connection closed by the other side", cause);
                } else {
                    connection.close("Exception in " + connection + ", thread=" + Thread.currentThread().getName(), cause);
                }
            }
        }
    }


    private void initializeToCluster(ClientConnection connection) {
        ClientMessage clientMessage = encodeAuthenticationRequest();
        ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, null, connection);
        ClientInvocationFuture invocationFuture = clientInvocation.invokeUrgent();

        ClientMessage response;
        try {
            response = invocationFuture.get(authenticationTimeout, MILLISECONDS);
        } catch (Exception e) {
            if (logger.isFinestEnabled()) {
                logger.finest("Authentication of " + connection + " failed.", e);
            }
            connection.close("Failed to authenticate connection", e);
            throw rethrow(e);
        }

        ClientAuthenticationCodec.ResponseParameters result = ClientAuthenticationCodec.decodeResponse(response);
        AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(result.status);
        switch (authenticationStatus) {
            case AUTHENTICATED:
                handleAuthResponse(connection, result);
                break;
            case CREDENTIALS_FAILED:
                throw new AuthenticationException("Invalid credentials!");
            case NOT_ALLOWED_IN_CLUSTER:
                throw new ClientNotAllowedInClusterException("Client is not allowed in the cluster");
            default:
                throw new AuthenticationException("Authentication status code not supported. status: "
                        + authenticationStatus);
        }
    }

    private void handleAuthResponse(ClientConnection connection, ClientAuthenticationCodec.ResponseParameters result) {
        checkPartitionCount(result.partitionCount);
        connection.setConnectedServerVersion(result.serverHazelcastVersion);
        connection.setRemoteEndpoint(result.address);
        UUID newClusterId = result.clusterId;

        if (logger.isFineEnabled()) {
            logger.fine("Checking the cluster id.Old: " + this.clusterId + ", new: " + newClusterId);
        }

        boolean isFirstConnectionAfterDisconnection = connectionCount.incrementAndGet() == 1;
        boolean changedCluster = false;
        if (isFirstConnectionAfterDisconnection) {
            changedCluster = clusterId != null && !newClusterId.equals(clusterId);
            clusterId = newClusterId;
        }

        if (changedCluster) {
            client.onClusterRestart();
        }
        activeConnections.put(resolveAddress(result.address), connection);
        fireConnectionAddedEvent(connection);

        logger.info("Authenticated with server " + result.address + ", server version:" + connection
                .getConnectedServerVersion() + " Local address: " + connection.getLocalSocketAddress());

        if (changedCluster) {
            clusterConnectionExecutor.execute(() -> sendStatesToCluster(newClusterId));
        } else if (isFirstConnectionAfterDisconnection) {
            state.set(ClusterState.CONNECTED);
            fireLifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED);
        }

        // Check if connection is closed by remote before authentication complete, if that is the case
        // we need to remove it back from active connections.
        // Race description from https://github.com/hazelcast/hazelcast/pull/8832.(A little bit changed)
        // - open a connection client -> member
        // - send auth message
        // - receive auth reply -> reply processing is offloaded to an executor. Did not start to run yet.
        // - member closes the connection -> the connection is trying to removed from map
        //                                                     but it was not there to begin with
        // - the executor start processing the auth reply -> it put the connection to the connection map.
        // - we end up with a closed connection in activeConnections map
        if (!connection.isAlive()) {
            removeFromActiveConnections(connection);
        }
    }

    private ClientMessage encodeAuthenticationRequest() {
        InternalSerializationService ss = client.getSerializationService();
        byte serializationVersion = ss.getVersion();

        CandidateClusterContext currentClusterContext = discoveryService.current();
        Credentials credentials = currentClusterContext.getCredentialsFactory().newCredentials();
        String clusterName = currentClusterContext.getClusterName();
        currentCredentials = credentials;

        if (credentials instanceof PasswordCredentials) {
            PasswordCredentials cr = (PasswordCredentials) credentials;
            return ClientAuthenticationCodec
                    .encodeRequest(clusterName, cr.getName(), cr.getPassword(), clientUuid, connectionType,
                            serializationVersion, BuildInfoProvider.getBuildInfo().getVersion(), client.getName(), labels);
        } else {
            byte[] secretBytes;
            if (credentials instanceof TokenCredentials) {
                secretBytes = ((TokenCredentials) credentials).getToken();
            } else {
                secretBytes = ss.toData(credentials).toByteArray();
            }
            return ClientAuthenticationCustomCodec.encodeRequest(clusterName, secretBytes, clientUuid, connectionType,
                    serializationVersion, BuildInfoProvider.getBuildInfo().getVersion(), client.getName(), labels);
        }
    }

    protected void checkClientActive() {
        if (!client.getLifecycleService().isRunning()) {
            throw new HazelcastClientNotActiveException();
        }
    }

    private void checkPartitionCount(int newPartitionCount) {
        if (partitionCount == -1) {
            partitionCount = newPartitionCount;
        } else if (partitionCount != newPartitionCount) {
            throw new ClientNotAllowedInClusterException("Client can not work with this cluster "
                    + " because it has a different partition count. "
                    + "Partition count client expects :" + partitionCount
                    + ", Member partition count:" + newPartitionCount);
        }
    }

    private boolean checkFailoverSupport(Connection connection) {
        if (!failoverConfigProvided) {
            return true;
        }
        ClientMessage isFailoverSupportedMessage = ClientIsFailoverSupportedCodec.encodeRequest();
        ClientInvocationFuture future = new ClientInvocation(client, isFailoverSupportedMessage, null, connection).invoke();
        try {
            boolean isAllowed = ClientIsFailoverSupportedCodec.decodeResponse(future.get()).response;
            if (!isAllowed) {
                logger.warning("Cluster does not support failover. "
                        + "This feature is available in Hazelcast Enterprise");
            }
            return isAllowed;
        } catch (InterruptedException | ExecutionException e) {
            logger.warning("Cluster did not answer to failover support query. ", e);
            return false;
        }
    }

    private InetSocketAddress resolveAddress(Address target) {
        return ConcurrencyUtil.getOrPutIfAbsent(inetSocketAddressCache, target, arg -> {
            try {
                return new InetSocketAddress(target.getInetAddress(), target.getPort());
            } catch (UnknownHostException e) {
                throw rethrow(e);
            }
        });
    }

    private void sendStatesToCluster(UUID targetClusterId) {
        try {
            //trying to prevent sending the state twice to same cluster
            if (targetClusterId.equals(clusterId)) {
                //still possible to send state twice to same cluster, since sendState is idempotent, there is no problem
                client.sendStateToCluster();
                state.set(ClusterState.CONNECTED);
                fireLifecycleEvent(LifecycleEvent.LifecycleState.CLIENT_CONNECTED);
            }
        } catch (Exception e) {
            String clusterName = discoveryService.current().getClusterName();
            logger.warning("Got exception when trying to send state to cluster. ", e);
            if (targetClusterId.equals(clusterId)) {
                logger.warning("Retrying sending state to cluster with uuid" + targetClusterId + ", name " + clusterName);
                clusterConnectionExecutor.execute(() -> sendStatesToCluster(targetClusterId));
            }
        }
    }
}

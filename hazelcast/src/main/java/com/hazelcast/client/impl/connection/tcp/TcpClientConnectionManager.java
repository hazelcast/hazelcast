/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientNotAllowedInClusterException;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.ClientLoggingService;
import com.hazelcast.client.impl.clientside.ClusterDiscoveryService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.LifecycleServiceImpl;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.impl.spi.impl.ClientPartitionServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER;
import static com.hazelcast.client.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.client.properties.ClientProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.client.properties.ClientProperty.IO_WRITE_THROUGH_ENABLED;
import static com.hazelcast.client.properties.ClientProperty.SHUFFLE_MEMBER_LIST;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
public class TcpClientConnectionManager implements ClientConnectionManager {

    private static final int DEFAULT_SMART_CLIENT_THREAD_COUNT = 3;
    private static final int EXECUTOR_CORE_POOL_SIZE = 10;
    private static final int SMALL_MACHINE_PROCESSOR_COUNT = 8;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    private final AtomicBoolean isAlive = new AtomicBoolean();
    private final ILogger logger;
    private final int connectionTimeoutMillis;
    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<Address, InetSocketAddress> inetSocketAddressCache = new ConcurrentHashMap<>();
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
    private final ScheduledExecutorService executor;
    private final boolean shuffleMemberList;
    private final WaitStrategy waitStrategy;
    private final ClusterDiscoveryService clusterDiscoveryService;

    private final boolean asyncStart;
    private final ReconnectMode reconnectMode;
    private final LoadBalancer loadBalancer;
    private final boolean isSmartRoutingEnabled;
    private final Runnable connectToAllClusterMembersTask = new ConnectToAllClusterMembersTask();
    private volatile Credentials currentCredentials;

    // following fields are updated inside synchronized(clientStateMutex)
    private final Object clientStateMutex = new Object();
    private final ConcurrentMap<UUID, TcpClientConnection> activeConnections = new ConcurrentHashMap<>();
    private volatile UUID clusterId;
    private volatile ClientState clientState = ClientState.INITIAL;
    private volatile boolean connectToClusterTaskSubmitted;

    private enum ClientState {
        /**
         * Clients start with this state. Once a client connects to a cluster,
         * it directly switches to {@link #INITIALIZED_ON_CLUSTER} instead of
         * {@link #CONNECTED_TO_CLUSTER} because on startup a client has no
         * local state to send to the cluster.
         */
        INITIAL,

        /**
         * When a client switches to a new cluster, it moves to this state.
         * It means that the client has connected to a new cluster but not sent
         * its local state to the new cluster yet.
         */
        CONNECTED_TO_CLUSTER,

        /**
         * When a client sends its local state to the cluster it has connected,
         * it switches to this state. When a client loses all connections to
         * the current cluster and connects to a new cluster, its state goes
         * back to {@link #CONNECTED_TO_CLUSTER}.
         * <p>
         * Invocations are allowed in this state.
         */
        INITIALIZED_ON_CLUSTER
    }

    public TcpClientConnectionManager(HazelcastClientInstanceImpl client) {
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
        this.executor = createExecutorService();
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.clusterDiscoveryService = client.getClusterDiscoveryService();
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
            if (isSmartRoutingEnabled && RuntimeAvailableProcessors.get() > SMALL_MACHINE_PROCESSOR_COUNT) {
                inputThreads = DEFAULT_SMART_CLIENT_THREAD_COUNT;
            } else {
                inputThreads = 1;
            }
        } else {
            inputThreads = configuredInputThreads;
        }

        int outputThreads;
        if (configuredOutputThreads == -1) {
            if (isSmartRoutingEnabled && RuntimeAvailableProcessors.get() > SMALL_MACHINE_PROCESSOR_COUNT) {
                outputThreads = DEFAULT_SMART_CLIENT_THREAD_COUNT;
            } else {
                outputThreads = 1;
            }
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
                        .inputThreadAffinity(newSystemThreadAffinity("hazelcast.client.io.input.thread.affinity"))
                        .outputThreadCount(outputThreads)
                        .outputThreadAffinity(newSystemThreadAffinity("hazelcast.client.io.output.thread.affinity"))
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .writeThroughEnabled(properties.getBoolean(IO_WRITE_THROUGH_ENABLED))
                        .concurrencyDetection(client.getConcurrencyDetection())
        );
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
            executor.scheduleWithFixedDelay(connectToAllClusterMembersTask, 1, 1, TimeUnit.SECONDS);
        }
    }

    public void connectToAllClusterMembers() {
        if (!isSmartRoutingEnabled) {
            return;
        }

        for (Member member : client.getClientClusterService().getMemberList()) {
            try {
                getOrConnect(member.getAddress());
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
        executor.shutdownNow();
        ClientExecutionServiceImpl.awaitExecutorTermination("cluster", executor, logger);
        for (Connection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }

        stopNetworking();
        connectionListeners.clear();
        heartbeat.shutdown();
        clusterDiscoveryService.current().destroy();
    }

    protected void stopNetworking() {
        networking.shutdown();
    }

    private void connectToCluster() {
        clusterDiscoveryService.current().start();

        if (asyncStart) {
            submitConnectToClusterTask();
        } else {
            doConnectToCluster();
        }
    }

    private void submitConnectToClusterTask() {
        // called in synchronized(clusterStateMutex)

        if (connectToClusterTaskSubmitted) {
            return;
        }

        executor.submit(() -> {
            try {
                doConnectToCluster();
                synchronized (clientStateMutex) {
                    connectToClusterTaskSubmitted = false;
                    if (activeConnections.isEmpty()) {
                        if (logger.isFineEnabled()) {
                            logger.warning("No connection to cluster: " + clusterId);
                        }

                        submitConnectToClusterTask();
                    }
                }
            } catch (Throwable e) {
                logger.warning("Could not connect to any cluster, shutting down the client: " + e.getMessage());
                shutdownWithExternalThread();
            }
        });

        connectToClusterTaskSubmitted = true;
    }

    private void doConnectToCluster() {
        CandidateClusterContext currentContext = clusterDiscoveryService.current();

        logger.info("Trying to connect to cluster: " + currentContext.getClusterName());

        // try the current cluster
        if (doConnectToCandidateCluster(currentContext)) {
            return;
        }

        // try the next cluster
        if (clusterDiscoveryService.tryNextCluster(this::destroyCurrentClusterConnectionAndTryNextCluster)) {
            return;
        }

        // notify when no succeeded cluster connection is found
        String msg = client.getLifecycleService().isRunning()
                ? "Unable to connect to any cluster." : "Client is being shutdown.";
        throw new IllegalStateException(msg);
    }

    private Boolean destroyCurrentClusterConnectionAndTryNextCluster(CandidateClusterContext currentContext,
                                                                     CandidateClusterContext nextContext) {
        currentContext.destroy();

        client.onClusterChange();

        nextContext.start();

        ((ClientLoggingService) client.getLoggingService()).updateClusterName(nextContext.getClusterName());

        logger.info("Trying to connect to next cluster: " + nextContext.getClusterName());

        if (doConnectToCandidateCluster(nextContext)) {
            client.waitForInitialMembershipEvents();
            fireLifecycleEvent(CLIENT_CHANGED_CLUSTER);
            return true;
        }
        return false;
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

    private void fireLifecycleEvent(LifecycleState state) {
        LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        lifecycleService.fireLifecycleEvent(state);
    }

    private boolean doConnectToCandidateCluster(CandidateClusterContext context) {
        Set<Address> triedAddresses = new HashSet<>();
        try {
            waitStrategy.reset();
            do {
                Collection<Address> addresses = getPossibleMemberAddresses(context.getAddressProvider());
                for (Address address : addresses) {
                    checkClientActive();
                    triedAddresses.add(address);

                    Connection connection = connect(address);
                    if (connection != null) {
                        return true;
                    }
                }
                // If the address providers load no addresses (which seems to be possible), then the above loop is not entered
                // and the lifecycle check is missing, hence we need to repeat the same check at this point.
                checkClientActive();
            } while (waitStrategy.sleep());
        } catch (ClientNotAllowedInClusterException | InvalidConfigurationException e) {
            logger.warning("Stopped trying on the cluster: " + context.getClusterName()
                    + " reason: " + e.getMessage());
        }

        logger.info("Unable to connect to any address from the cluster with name: " + context.getClusterName()
                + ". The following addresses were tried: " + triedAddresses);
        return false;
    }

    @Override
    public void checkInvocationAllowed() throws IOException {
        ClientState state = this.clientState;
        if (state == ClientState.INITIALIZED_ON_CLUSTER && activeConnections.size() > 0) {
            return;
        }

        if (state == ClientState.INITIAL) {
            if (asyncStart) {
                throw new HazelcastClientOfflineException();
            } else {
                throw new IOException("No connection found to cluster since the client is starting.");
            }
        } else if (ReconnectMode.ASYNC.equals(reconnectMode)) {
            throw new HazelcastClientOfflineException();
        } else {
            throw new IOException("No connection found to cluster.");
        }
    }

    Collection<Address> getPossibleMemberAddresses(AddressProvider addressProvider) {
        List<Address> memberAddresses = client.getClientClusterService()
                .getMemberList()
                .stream()
                .map(Member::getAddress)
                .collect(toList());
        if (shuffleMemberList) {
            Collections.shuffle(memberAddresses);
        }

        Collection<Address> addresses = new LinkedHashSet<>(memberAddresses);

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

            addresses.addAll(result.primary());
            addresses.addAll(result.secondary());
        } catch (NullPointerException e) {
            throw e;
        } catch (Exception e) {
            logger.warning("Exception from AddressProvider: " + clusterDiscoveryService, e);
        }

        return addresses;
    }

    private void shutdownWithExternalThread() {
        new Thread(() -> {
            try {
                client.getLifecycleService().shutdown();
            } catch (Exception exception) {
                logger.severe("Exception during client shutdown", exception);
            }
        }, client.getName() + ".clientShutdown-").start();
    }

    @Override
    public Collection<ClientConnection> getActiveConnections() {
        return (Collection) activeConnections.values();
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
    public ClientConnection getConnection(@Nonnull UUID uuid) {
        return activeConnections.get(uuid);
    }

    private TcpClientConnection getConnection(@Nonnull Address address) {
        for (TcpClientConnection connection : activeConnections.values()) {
            if (connection.getRemoteAddress().equals(address)) {
                return connection;
            }
        }
        return null;
    }

    TcpClientConnection getOrConnect(@Nonnull Address address) {
        checkClientActive();
        TcpClientConnection connection = getConnection(address);
        if (connection != null) {
            return connection;
        }

        synchronized (resolveAddress(address)) {
            // this critical section is used for making a single connection
            // attempt to the given address at a time.
            connection = getConnection(address);
            if (connection != null) {
                return connection;
            }

            address = translate(address);
            connection = createSocketConnection(address);
            authenticateOnCluster(connection);
            return connection;
        }
    }

    private void fireConnectionAddedEvent(TcpClientConnection connection) {
        for (ConnectionListener connectionListener : connectionListeners) {
            connectionListener.connectionAdded(connection);
        }
    }

    private void fireConnectionRemovedEvent(TcpClientConnection connection) {
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
            Integer port = outboundPorts.removeFirst();
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

            if (ex != null) {
                throw ex;
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected TcpClientConnection createSocketConnection(Address target) {
        CandidateClusterContext currentClusterContext = clusterDiscoveryService.current();
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();

            bindSocketToPort(socket);

            Channel channel = networking.register(currentClusterContext.getChannelInitializer(), socketChannel, true);
            channel.attributeMap().put(Address.class, target);
            channel.connect(resolveAddress(target), connectionTimeoutMillis);

            TcpClientConnection connection = new TcpClientConnection(client, connectionIdGen.incrementAndGet(), channel);

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
        CandidateClusterContext currentContext = clusterDiscoveryService.current();
        AddressProvider addressProvider = currentContext.getAddressProvider();
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

    void onConnectionClose(TcpClientConnection connection) {
        Address endpoint = connection.getRemoteAddress();
        UUID memberUuid = connection.getRemoteUuid();

        if (endpoint == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying " + connection + ", but it has end-point set to null "
                        + "-> not removing it from a connection map");
            }
            return;
        }

        synchronized (clientStateMutex) {
            if (activeConnections.remove(memberUuid, connection)) {
                logger.info("Removed connection to endpoint: " + endpoint + ":" + memberUuid + ", connection: " + connection);
                if (activeConnections.isEmpty()) {
                    if (clientState == ClientState.INITIALIZED_ON_CLUSTER) {
                        fireLifecycleEvent(LifecycleState.CLIENT_DISCONNECTED);
                    }

                    triggerClusterReconnection();
                }

                fireConnectionRemovedEvent(connection);
            } else if (logger.isFinestEnabled()) {
                logger.finest("Destroying a connection, but there is no mapping " + endpoint + ":" + memberUuid
                        + " -> " + connection + " in the connection map.");
            }
        }
    }

    private void triggerClusterReconnection() {
        if (reconnectMode == OFF) {
            logger.info("RECONNECT MODE is off. Shutting down the client.");
            shutdownWithExternalThread();
            return;
        }

        if (client.getLifecycleService().isRunning()) {
            try {
                submitConnectToClusterTask();
            } catch (RejectedExecutionException r) {
                shutdownWithExternalThread();
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
        for (TcpClientConnection activeConnection : activeConnections.values()) {
            activeConnection.close(null, new TargetDisconnectedException("Closing since client is switching cluster"));
        }

        inetSocketAddressCache.clear();
    }

    @Override
    public ClientConnection getRandomConnection(boolean dataMember) {
        // Try getting the connection from the load balancer, if smart routing is enabled
        if (isSmartRoutingEnabled) {
            ClientConnection connection = getConnectionFromLoadBalancer(dataMember);

            if (connection != null) {
                return connection;
            }
        }

        // Otherwise iterate over connections and return the very first valid
        Iterator<Map.Entry<UUID, TcpClientConnection>> iterator = activeConnections.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<UUID, TcpClientConnection> connectionEntry = iterator.next();

            if (dataMember) {
                UUID memberId = connectionEntry.getKey();

                Member member = client.getClientClusterService().getMember(memberId);

                if (member == null || member.isLiteMember()) {
                    continue;
                }
            }

            return connectionEntry.getValue();
        }

        // Failed to get a connection
        return null;
    }

    private ClientConnection getConnectionFromLoadBalancer(boolean dataMember) {
        Member member;

        if (dataMember) {
            if (loadBalancer.canGetNextDataMember()) {
                member = loadBalancer.nextDataMember();
            } else {
                member = null;
            }
        } else {
            member = loadBalancer.next();
        }

        // Failed to get member
        if (member == null) {
            return null;
        }

        return getConnection(member.getUuid());
    }

    private void authenticateOnCluster(TcpClientConnection connection) {
        Address memberAddress = connection.getInitAddress();
        ClientMessage request = encodeAuthenticationRequest(memberAddress);
        ClientInvocationFuture future = new ClientInvocation(client, request, null, connection).invokeUrgent();
        ClientAuthenticationCodec.ResponseParameters response;
        try {
            response = ClientAuthenticationCodec.decodeResponse(future.get(authenticationTimeout, MILLISECONDS));
        } catch (Exception e) {
            connection.close("Failed to authenticate connection", e);
            throw rethrow(e);
        }

        AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(response.status);
        if (failoverConfigProvided && !response.failoverSupported) {
            logger.warning("Cluster does not support failover. This feature is available in Hazelcast Enterprise");
            authenticationStatus = NOT_ALLOWED_IN_CLUSTER;
        }
        switch (authenticationStatus) {
            case AUTHENTICATED:
                handleSuccessfulAuth(connection, response);
                break;
            case CREDENTIALS_FAILED:
                AuthenticationException authException = new AuthenticationException("Authentication failed. The configured "
                        + "cluster name on the client (see ClientConfig.setClusterName()) does not match the one configured in "
                        + "the cluster or the credentials set in the Client security config could not be authenticated");
                connection.close("Failed to authenticate connection", authException);
                throw authException;
            case NOT_ALLOWED_IN_CLUSTER:
                ClientNotAllowedInClusterException notAllowedException =
                        new ClientNotAllowedInClusterException("Client is not allowed in the cluster");
                connection.close("Failed to authenticate connection", notAllowedException);
                throw notAllowedException;
            default:
                AuthenticationException exception =
                        new AuthenticationException("Authentication status code not supported. status: " + authenticationStatus);
                connection.close("Failed to authenticate connection", exception);
                throw exception;
        }
    }

    private void handleSuccessfulAuth(TcpClientConnection connection, ClientAuthenticationCodec.ResponseParameters response) {
        synchronized (clientStateMutex) {
            checkPartitionCount(response.partitionCount);
            connection.setConnectedServerVersion(response.serverHazelcastVersion);
            connection.setRemoteAddress(response.address);
            connection.setRemoteUuid(response.memberUuid);

            UUID newClusterId = response.clusterId;

            if (logger.isFineEnabled()) {
                logger.fine("Checking the cluster: " + newClusterId + ", current cluster: " + this.clusterId);
            }

            boolean initialConnection = activeConnections.isEmpty();
            boolean changedCluster = initialConnection && this.clusterId != null && !newClusterId.equals(this.clusterId);
            if (changedCluster) {
                logger.warning("Switching from current cluster: " + this.clusterId + " to new cluster: " + newClusterId);
                client.onClusterRestart();
            }

            activeConnections.put(response.memberUuid, connection);

            if (initialConnection) {
                clusterId = newClusterId;
                if (changedCluster) {
                    clientState = ClientState.CONNECTED_TO_CLUSTER;
                    executor.execute(() -> initializeClientOnCluster(newClusterId));
                } else {
                    clientState = ClientState.INITIALIZED_ON_CLUSTER;
                    fireLifecycleEvent(LifecycleState.CLIENT_CONNECTED);
                }
            }

            logger.info("Authenticated with server " + response.address + ":" + response.memberUuid
                    + ", server version: " + response.serverHazelcastVersion
                    + ", local address: " + connection.getLocalSocketAddress());

            fireConnectionAddedEvent(connection);
        }

        // It could happen that this connection is already closed and
        // onConnectionClose() is called even before the synchronized block
        // above is executed. In this case, now we have a closed but registered
        // connection. We do a final check here to remove this connection
        // if needed.
        if (!connection.isAlive()) {
            onConnectionClose(connection);
        }
    }

    private ClientMessage encodeAuthenticationRequest(Address toAddress) {
        InternalSerializationService ss = client.getSerializationService();
        byte serializationVersion = ss.getVersion();

        CandidateClusterContext currentContext = clusterDiscoveryService.current();
        Credentials credentials = currentContext.getCredentialsFactory().newCredentials(toAddress);
        String clusterName = currentContext.getClusterName();
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
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        if (!partitionService.checkAndSetPartitionCount(newPartitionCount)) {
            throw new ClientNotAllowedInClusterException("Client can not work with this cluster"
                    + " because it has a different partition count. "
                    + "Expected partition count: " + partitionService.getPartitionCount()
                    + ", Member partition count: " + newPartitionCount);
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

    private void initializeClientOnCluster(UUID targetClusterId) {
        // submitted inside synchronized(clientStateMutex)

        try {
            synchronized (clientStateMutex) {
                if (!targetClusterId.equals(clusterId)) {
                    logger.warning("Won't send client state to cluster: " + targetClusterId
                            + " Because switched to a new cluster: " + clusterId);
                    return;
                }
            }

            client.sendStateToCluster();

            synchronized (clientStateMutex) {
                if (targetClusterId.equals(clusterId)) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Client state is sent to cluster: " + targetClusterId);
                    }

                    clientState = ClientState.INITIALIZED_ON_CLUSTER;
                    fireLifecycleEvent(LifecycleState.CLIENT_CONNECTED);
                } else if (logger.isFineEnabled()) {
                    logger.warning("Cannot set client state to " + ClientState.INITIALIZED_ON_CLUSTER
                            + " because current cluster id: " + this.clusterId + " is different than expected cluster id: "
                            + targetClusterId);
                }
            }
        } catch (Exception e) {
            String clusterName = clusterDiscoveryService.current().getClusterName();
            logger.warning("Failure during sending state to the cluster.", e);
            synchronized (clientStateMutex) {
                if (targetClusterId.equals(clusterId)) {
                    if (logger.isFineEnabled()) {
                        logger.warning("Retrying sending state to the cluster: " + targetClusterId + ", name: " + clusterName);
                    }

                    executor.execute(() -> initializeClientOnCluster(targetClusterId));
                }
            }
        }
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

                Connection connection = (Connection) channel.attributeMap().get(TcpClientConnection.class);
                if (cause instanceof EOFException) {
                    connection.close("Connection closed by the other side", cause);
                } else {
                    connection.close("Exception in " + connection + ", thread=" + Thread.currentThread().getName(), cause);
                }
            }
        }
    }

    private class ConnectToAllClusterMembersTask implements Runnable {

        private Set<Address> connectingAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());

        @Override
        public void run() {
            if (!client.getLifecycleService().isRunning()) {
                return;
            }

            for (Member member : client.getClientClusterService().getMemberList()) {
                Address address = member.getAddress();

                if (client.getLifecycleService().isRunning() && getConnection(address) == null
                        && connectingAddresses.add(address)) {
                    // submit a task for this address only if there is no
                    // another connection attempt for it
                    executor.submit(() -> {
                        try {
                            if (!client.getLifecycleService().isRunning()) {
                                return;
                            }
                            if (getConnection(member.getUuid()) == null) {
                                getOrConnect(address);
                            }
                        } catch (Exception e) {
                            EmptyStatement.ignore(e);
                        } finally {
                            connectingAddresses.remove(address);
                        }
                    });
                }
            }
        }
    }
}

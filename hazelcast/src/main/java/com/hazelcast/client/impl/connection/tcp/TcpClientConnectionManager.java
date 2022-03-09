/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.function.BiFunctionEx;
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
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.PasswordCredentials;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.sql.impl.QueryUtils;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static com.hazelcast.client.config.ConnectionRetryConfig.DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS;
import static com.hazelcast.client.config.ConnectionRetryConfig.FAILOVER_CLIENT_DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS;
import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER;
import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_TIMEOUT;
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

/**
 * Implementation of {@link ClientConnectionManager}.
 */
public class TcpClientConnectionManager implements ClientConnectionManager, MembershipListener {

    private static final int DEFAULT_SMART_CLIENT_THREAD_COUNT = 3;
    private static final int SMALL_MACHINE_PROCESSOR_COUNT = 8;
    private static final int SQL_CONNECTION_RANDOM_ATTEMPTS = 10;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    private final AtomicBoolean isAlive = new AtomicBoolean();
    private final ILogger logger;
    private final int connectionTimeoutMillis;
    private final HazelcastClientInstanceImpl client;
    private final Collection<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();
    private final NioNetworking networking;
    private final long authenticationTimeout;
    private final String connectionType;
    private final UUID clientUuid = UuidUtil.newUnsecureUUID();
    // outboundPorts is accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<>();
    private final Set<String> labels;
    private final int outboundPortCount;
    private final boolean failoverConfigProvided;
    private final ExecutorService clusterExecutor;
    private final boolean shuffleMemberList;
    private final WaitStrategy waitStrategy;
    private final ClusterDiscoveryService clusterDiscoveryService;
    private final boolean asyncStart;
    private final ReconnectMode reconnectMode;
    private final LoadBalancer loadBalancer;
    private final boolean isSmartRoutingEnabled;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private volatile Credentials currentCredentials;
    private final ConcurrentMap<UUID, TcpClientConnection> activeConnections = new ConcurrentHashMap<>();
    private volatile UUID clusterId;
    private volatile ClientState clientState = ClientState.INITIAL;

    private enum ClientState {
        /**
         * Clients start with this state. Once a client connects to a cluster,
         * it directly switches to {@link #CONNECTED} state.
         */
        INITIAL,

        /**
         * When a client switches to a new cluster, it moves to this state.
         * It means that the client has connected to a new cluster but not sent
         * its local state to the new cluster yet.
         */
        DISCONNECTED,

        /**
         * When a client sends its local state to the cluster it has connected,
         * it switches to this state.
         * <p>
         * Invocations are allowed in this state.
         */
        CONNECTED;
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
        this.authenticationTimeout = client.getProperties().getPositiveMillisOrDefault(HEARTBEAT_TIMEOUT);
        this.failoverConfigProvided = client.getFailoverConfig() != null;
        this.clusterExecutor = createClusterExecutorService();
        this.clusterDiscoveryService = client.getClusterDiscoveryService();
        this.waitStrategy = initializeWaitStrategy(client.getClientConfig());
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.isSmartRoutingEnabled = client.getClientConfig().getNetworkConfig().isSmartRouting();
        this.asyncStart = client.getClientConfig().getConnectionStrategyConfig().isAsyncStart();
        this.reconnectMode = client.getClientConfig().getConnectionStrategyConfig().getReconnectMode();
    }

    private int initConnectionTimeoutMillis() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        final int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
    }

    private ExecutorService createClusterExecutorService() {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        SingleExecutorThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, client.getName() + ".cluster");
        return Executors.newSingleThreadExecutor(threadFactory);
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
        ConnectionRetryConfig retryConfig = clientConfig
                .getConnectionStrategyConfig()
                .getConnectionRetryConfig();

        long clusterConnectTimeout = retryConfig.getClusterConnectTimeoutMillis();

        if (clusterConnectTimeout == DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS) {
            // If no value is provided, or set to -1 explicitly,
            // use a predefined timeout value for the failover client
            // and infinite for the normal client.
            if (failoverConfigProvided) {
                clusterConnectTimeout = FAILOVER_CLIENT_DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS;
            } else {
                clusterConnectTimeout = Long.MAX_VALUE;
            }
        }

        return new WaitStrategy(retryConfig.getInitialBackoffMillis(),
                retryConfig.getMaxBackoffMillis(),
                retryConfig.getMultiplier(),
                clusterConnectTimeout,
                retryConfig.getJitter(), logger);
    }

    public synchronized void start() {
        if (!isAlive.compareAndSet(false, true)) {
            return;
        }
        startNetworking();
    }

    public void tryConnectToAllClusterMembers(boolean sync) {
        if (isSmartRoutingEnabled && sync) {
            connectToAllClusterMembers();
        }
    }

    private void clusterThreadLoop() {
        while (isAlive.get()) {
            try {
                Runnable runnable = taskQueue.poll(1, TimeUnit.SECONDS);
                if (!isAlive.get()) {
                    break;
                }
                if (runnable != null) {
                    runnable.run();
                } else if (isSmartRoutingEnabled) {
                    connectToAllClusterMembers();
                }
            } catch (Exception e) {
                logger.warning("Exception occurred while executing cluster task", e);
            }
        }
    }

    private void handleConnectionClose(TcpClientConnection connection) {
        client.getInvocationService().onConnectionClose(connection);
        Address endpoint = connection.getRemoteAddress();
        UUID memberUuid = connection.getRemoteUuid();
        if (endpoint == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying " + connection + ", but it has end-point set to null "
                        + "-> not removing it from a connection map");
            }
            return;
        }

        if (activeConnections.remove(memberUuid, connection)) {
            logger.info("Removed connection to endpoint: " + endpoint + ":" + memberUuid + ", connection: " + connection);

            fireConnectionEvent(connection, false);
        } else if (logger.isFinestEnabled()) {
            logger.finest("Destroying a connection, but there is no mapping " + endpoint + ":" + memberUuid
                    + " -> " + connection + " in the connection map.");
        }

        if (!activeConnections.isEmpty()) {
            //if there are still active connections, we don't have to do anything
            return;
        }

        // if there are no more connections, we are disconnected only if we were connected to a cluster
        if (clientState == ClientState.CONNECTED) {
            fireLifecycleEvent(LifecycleState.CLIENT_DISCONNECTED);
        }

        clientState = ClientState.DISCONNECTED;

        if (reconnectMode == OFF) {
            logger.info("RECONNECT MODE is off. Shutting down the client.");
            shutdownWithExternalThread();
            return;
        }

        retryConnectToCluster();
    }

    protected void startNetworking() {
        networking.restart();
    }

    public synchronized void shutdown() {
        if (!isAlive.compareAndSet(true, false)) {
            return;
        }
        //add a task to the queue to make sure that the cluster loop is stopped
        taskQueue.add(() -> {
        });
        clusterExecutor.shutdownNow();
        ClientExecutionServiceImpl.awaitExecutorTermination("cluster", clusterExecutor, logger);
        for (Connection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }
        stopNetworking();
        connectionListeners.clear();
        clusterDiscoveryService.current().destroy();
    }

    protected void stopNetworking() {
        networking.shutdown();
    }

    public void initialConnectToCluster() {
        clusterDiscoveryService.current().start();
        if (asyncStart) {
            asyncConnectToCluster();
        } else {
            connectToCluster();
        }
        clusterExecutor.execute(this::clusterThreadLoop);
    }

    private void asyncConnectToCluster() {
        taskQueue.add(this::retryConnectToCluster);
    }

    private void retryConnectToCluster() {
        if (!client.getLifecycleService().isRunning()) {
            shutdownWithExternalThread();
        }
        try {
            connectToCluster();
        } catch (Throwable e) {
            logger.warning("Could not connect to any cluster, shutting down the client: " + e.getMessage());
            shutdownWithExternalThread();
        }
    }

    /**
     * If this throws exception it means instance is either not started or will be shutdown.
     */
    private void connectToCluster() {
        CandidateClusterContext currentContext = clusterDiscoveryService.current();
        logger.info("Trying to connect to cluster: " + currentContext.getClusterName());
        // try the current cluster
        AuthenticationResult result = doConnectToCandidateCluster(currentContext);
        if (result != null && checkClusterId(result) && onAuthenticatedToCluster(result)) {
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

    private boolean checkClusterId(AuthenticationResult result) {
        if (failoverConfigProvided && isClusterIdChanged(result.response.clusterId)) {
            // If failover is provided, and this single connection is established to a different cluster, while
            // we were trying to connect back to the same cluster, we should failover to the next cluster.
            // Otherwise, we force the failover logic to be used by throwing `ClientNotAllowedInClusterException`
            result.connection.close("Connection belongs to another cluster. "
                    + "Current cluster id" + clusterId + ", new cluster id " + result.response.clusterId
                    + "Closing this to open a connection to next cluster with the correct client config", null);
            return false;
        }
        return true;
    }

    private boolean destroyCurrentClusterConnectionAndTryNextCluster(CandidateClusterContext currentContext,
                                                                     CandidateClusterContext nextContext) {
        currentContext.destroy();
        client.onClusterChange();
        nextContext.start();
        ((ClientLoggingService) client.getLoggingService()).updateClusterName(nextContext.getClusterName());
        logger.info("Trying to connect to next cluster: " + nextContext.getClusterName());
        AuthenticationResult result = doConnectToCandidateCluster(nextContext);
        if (result != null && onAuthenticatedToCluster(result)) {
            client.waitForInitialMembershipEvents();
            fireLifecycleEvent(CLIENT_CHANGED_CLUSTER);
            return true;
        }
        return false;
    }

    private void fireLifecycleEvent(LifecycleState state) {
        LifecycleServiceImpl lifecycleService = (LifecycleServiceImpl) client.getLifecycleService();
        lifecycleService.fireLifecycleEvent(state);
    }

    private AuthenticationResult doConnectToCandidateCluster(CandidateClusterContext context) {
        Set<Address> triedAddresses = new HashSet<>();
        try {
            waitStrategy.reset();
            do {
                Set<Address> triedAddressesPerAttempt = new HashSet<>();

                List<Member> memberList = new ArrayList<>(client.getClientClusterService().getMemberList());
                if (shuffleMemberList) {
                    Collections.shuffle(memberList);
                }
                //try to connect to a member in the member list first
                for (Member member : memberList) {
                    checkClientActive();
                    triedAddressesPerAttempt.add(member.getAddress());
                    AuthenticationResult result = connect(translate(member));
                    if (result != null) {
                        return result;
                    }
                }
                //try to connect to a member given via config(explicit config/discovery mechanisms)
                for (Address address : getPossibleMemberAddresses(context.getAddressProvider())) {
                    checkClientActive();
                    if (!triedAddressesPerAttempt.add(address)) {
                        //if we can not add it means that it is already tried to be connected with the member list
                        continue;
                    }
                    AuthenticationResult result = connect(translate(address));
                    if (result != null) {
                        return result;
                    }
                }
                triedAddresses.addAll(triedAddressesPerAttempt);
                // If the address provider loads no addresses, then the above loop is not entered
                // and the lifecycle check is missing, hence we need to repeat the same check at this point.
                if (triedAddressesPerAttempt.isEmpty()) {
                    checkClientActive();
                }
            } while (waitStrategy.sleep());
        } catch (ClientNotAllowedInClusterException | InvalidConfigurationException e) {
            logger.warning("Stopped trying on the cluster: " + context.getClusterName()
                    + " reason: " + e.getMessage());
        }

        logger.info("Unable to connect to any address from the cluster with name: " + context.getClusterName()
                + ". The following addresses were tried: " + triedAddresses);
        return null;
    }

    @Override
    public String getConnectionType() {
        return connectionType;
    }

    @Override
    public void checkInvocationAllowed() throws IOException {
        ClientState state = this.clientState;
        if (state == ClientState.CONNECTED && activeConnections.size() > 0) {
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
        Collection<Address> addresses = new LinkedHashSet<>();
        try {
            Addresses result = addressProvider.loadAddresses();
            if (shuffleMemberList) {
                // The relative order between primary and secondary addresses should not be changed.
                // so we shuffle the lists separately and then add them to the final list so that
                // secondary addresses are not tried before all primary addresses have been tried.
                // Otherwise, we can get startup delays.
                Collections.shuffle(result.primary());
                Collections.shuffle(result.secondary());
            }

            addresses.addAll(result.primary());
            addresses.addAll(result.secondary());
        } catch (NullPointerException e) {
            throw e;
        } catch (Exception e) {
            logger.warning("Exception from AddressProvider: " + addressProvider, e);
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
    public Collection<Connection> getActiveConnections() {
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

    static class AuthenticationResult {
        TcpClientConnection connection;
        ClientAuthenticationCodec.ResponseParameters response;

        AuthenticationResult(TcpClientConnection connection, ClientAuthenticationCodec.ResponseParameters response) {
            this.connection = connection;
            this.response = response;
        }
    }

    AuthenticationResult connect(@Nonnull Address address) {
        try {
            logger.info("Trying to connect to " + address);
            TcpClientConnection connection = createSocketConnection(address);
            ClientAuthenticationCodec.ResponseParameters response = authenticate(connection);
            checkAuthenticationResponse(connection, response);
            connection.setConnectedServerVersion(response.serverHazelcastVersion);
            connection.setRemoteAddress(response.address);
            connection.setRemoteUuid(response.memberUuid);
            connection.setClusterUuid(response.clusterId);
            assert activeConnections.isEmpty() : "active connections should be empty when connection to cluster";
            return new AuthenticationResult(connection, response);
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

    private void fireConnectionEvent(TcpClientConnection connection, boolean isAdded) {
        ConcurrencyUtil.getDefaultAsyncExecutor().execute(() -> {
            for (ConnectionListener listener : connectionListeners) {
                if (isAdded) {
                    listener.connectionAdded(connection);
                } else {
                    listener.connectionRemoved(connection);
                }
            }
        });
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

            InetSocketAddress inetSocketAddress = new InetSocketAddress(target.getInetAddress(), target.getPort());
            channel.connect(inetSocketAddress, connectionTimeoutMillis);

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

    Address translate(Member member) {
        return translate(member, AddressProvider::translate);
    }

    private Address translate(Address address) {
        return translate(address, AddressProvider::translate);
    }

    private <T> Address translate(T target, BiFunctionEx<AddressProvider, T, Address> translateFunction) {
        CandidateClusterContext currentContext = clusterDiscoveryService.current();
        AddressProvider addressProvider = currentContext.getAddressProvider();
        try {
            Address translatedAddress = translateFunction.apply(addressProvider, target);
            if (translatedAddress == null) {
                throw new HazelcastException("Address Provider " + addressProvider.getClass()
                        + " could not translate " + target);
            }
            return translatedAddress;
        } catch (Exception e) {
            logger.warning("Failed to translate " + target + " via address provider " + e.getMessage());
            throw rethrow(e);
        }
    }

    void onConnectionClose(TcpClientConnection connection) {
        taskQueue.add(() -> handleConnectionClose(connection));
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
    }

    @Override
    public ClientConnection getRandomConnection() {
        // Try getting the connection from the load balancer, if smart routing is enabled
        if (isSmartRoutingEnabled) {
            Member member = loadBalancer.next();

            // Failed to get a member
            ClientConnection connection = member != null ? activeConnections.get(member.getUuid()) : null;
            if (connection != null) {
                return connection;
            }
        }
        // Otherwise, iterate over connections and return the first one
        for (Map.Entry<UUID, TcpClientConnection> connectionEntry : activeConnections.entrySet()) {
            return connectionEntry.getValue();
        }
        // Failed to get a connection
        return null;
    }

    @Override
    public ClientConnection getConnectionForSql() {
        if (isSmartRoutingEnabled) {
            // There might be a race - the chosen member might be just connected or disconnected - try a
            // couple of times, the memberOfLargerSameVersionGroup returns a random connection,
            // we might be lucky...
            for (int i = 0; i < SQL_CONNECTION_RANDOM_ATTEMPTS; i++) {
                Member member = QueryUtils.memberOfLargerSameVersionGroup(
                        client.getClientClusterService().getMemberList(), null);
                if (member == null) {
                    break;
                }
                ClientConnection connection = activeConnections.get(member.getUuid());
                if (connection != null) {
                    return connection;
                }
            }
        }

        // Otherwise iterate over connections and return the first one that's not to a lite member
        ClientConnection firstConnection = null;
        for (Map.Entry<UUID, TcpClientConnection> connectionEntry : activeConnections.entrySet()) {
            if (firstConnection == null) {
                firstConnection = connectionEntry.getValue();
            }
            UUID memberId = connectionEntry.getKey();
            Member member = client.getClientClusterService().getMember(memberId);
            if (member == null || member.isLiteMember()) {
                continue;
            }
            return connectionEntry.getValue();
        }

        // Failed to get a connection to a data member
        return firstConnection;
    }

    private ClientAuthenticationCodec.ResponseParameters authenticate(TcpClientConnection connection) {
        ClientInvocationFuture future = authenticateAsync(connection);
        try {
            return ClientAuthenticationCodec.decodeResponse(future.get(authenticationTimeout, MILLISECONDS));
        } catch (Exception e) {
            connection.close("Failed to authenticate connection", e);
            throw rethrow(e);
        }
    }

    private ClientInvocationFuture authenticateAsync(TcpClientConnection connection) {
        Address memberAddress = connection.getInitAddress();
        ClientMessage request = encodeAuthenticationRequest(memberAddress);
        return new ClientInvocation(client, request, null, connection).invokeUrgent();
    }

    private void onAuthenticatedToMember(TcpClientConnection connection,
                                         ClientAuthenticationCodec.ResponseParameters response) {
        TcpClientConnection existingConnection = activeConnections.get(response.memberUuid);
        if (existingConnection != null) {
            connection.close("Duplicate connection to same member with uuid : " + response.memberUuid, null);
            return;
        }

        if (isClusterIdChanged(response.clusterId)) {
            // We have a connection to wrong cluster.
            // We should not stay connected to this new connection.
            // Note that in some race scenarios we might close a connection that we can continue operating on.
            // In those cases, we rely on the fact that we will reopen the connections and continue. Here is one scenario:
            // 1. There were 2 members.
            // 2. The client is connected to the first one.
            // 3. While the client is trying to open the second connection, both members are restarted.
            // 4. In this case we will close the connection to the second member, thinking that it is not part of the
            // cluster we think we are in. We will reconnect to this member, and the connection is closed unnecessarily.
            // 5. The connection to the first cluster will be gone after that, and we will initiate reconnect to the cluster.
            String reason = "Connection does not belong to this cluster";
            connection.close(reason, null);
            throw new IllegalStateException(reason);
        }
        connectionAuthenticated(connection, response);
    }

    /**
     * @return false if post authentication actions like sending necessary states to cluster fails.
     * In this case, the caller should retry connecting to the cluster again.
     */
    private boolean onAuthenticatedToCluster(AuthenticationResult authenticationResult) {
        ClientAuthenticationCodec.ResponseParameters response = authenticationResult.response;
        TcpClientConnection connection = authenticationResult.connection;
        UUID newClusterId = response.clusterId;
        boolean clusterIdChanged = isClusterIdChanged(newClusterId);
        if (clusterIdChanged) {
            logger.warning("Switching from current cluster: " + this.clusterId + " to new cluster: " + newClusterId);
            client.onClusterConnect();
        }
        clusterId = newClusterId;
        connectionAuthenticated(connection, response);
        if (clusterIdChanged) {
            try {
                client.sendStateToCluster();
                if (logger.isFineEnabled()) {
                    logger.fine("Client state is sent to cluster: " + clusterId);
                }
            } catch (RuntimeException e) {
                logger.warning("Failure during sending state to the cluster. " + connection, e);
                activeConnections.remove(connection.getRemoteUuid());
                connection.close("Failure during sending state to the cluster.", null);
                return false;
            }
        }
        clientState = ClientState.CONNECTED;
        fireLifecycleEvent(LifecycleState.CLIENT_CONNECTED);
        return true;
    }

    private boolean isClusterIdChanged(UUID newClusterId) {
        if (logger.isFineEnabled()) {
            logger.fine("Checking the cluster: " + newClusterId + ", current cluster: " + this.clusterId);
        }
        // `clusterId` is `null` only at the start of the client.
        // `clusterId` is set by master when a cluster is started.
        // `clusterId` is not preserved during HotRestart.
        // In split brain, both sides have the same `clusterId`
        return this.clusterId != null && !newClusterId.equals(this.clusterId);
    }

    private void connectionAuthenticated(TcpClientConnection connection, ClientAuthenticationCodec.ResponseParameters response) {
        activeConnections.put(connection.getRemoteUuid(), connection);
        logger.info("Authenticated with server " + response.address + ":" + response.memberUuid
                + ", server version: " + response.serverHazelcastVersion
                + ", local address: " + connection.getLocalSocketAddress());
        fireConnectionEvent(connection, true);
    }

    /**
     * Checks the response from the server to see if authentication needs to be continued,
     * closes the connection and throws exception if the authentication needs to be cancelled.
     */
    private void checkAuthenticationResponse(TcpClientConnection connection,
                                             ClientAuthenticationCodec.ResponseParameters response) {
        AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(response.status);
        if (failoverConfigProvided && !response.failoverSupported) {
            logger.warning("Cluster does not support failover. This feature is available in Hazelcast Enterprise");
            authenticationStatus = NOT_ALLOWED_IN_CLUSTER;
        }
        switch (authenticationStatus) {
            case AUTHENTICATED:
                break;
            case CREDENTIALS_FAILED:
                AuthenticationException authException = new AuthenticationException("Authentication failed. The configured "
                        + "cluster name on the client (see ClientConfig.setClusterName()) does not match the one configured "
                        + "in the cluster or the credentials set in the Client security config could not be authenticated");
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
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        if (!partitionService.checkAndSetPartitionCount(response.partitionCount)) {
            ClientNotAllowedInClusterException exception =
                    new ClientNotAllowedInClusterException("Client can not work with this cluster"
                            + " because it has a different partition count. "
                            + "Expected partition count: " + partitionService.getPartitionCount()
                            + ", Member partition count: " + response.partitionCount);
            connection.close("Failed to authenticate connection", exception);
            throw exception;
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

    private static class ConnectionAttempt {
        UUID memberUuid;
        Future<ClientAuthenticationCodec.ResponseParameters> future;
        TcpClientConnection connection;

        ConnectionAttempt(UUID memberUuid, Future<ClientAuthenticationCodec.ResponseParameters> future,
                          TcpClientConnection connection) {
            this.memberUuid = memberUuid;
            this.future = future;
            this.connection = connection;
        }
    }

    public void connectToAllClusterMembers() {
        if (!client.getLifecycleService().isRunning()) {
            return;
        }

        if (clientState != ClientState.CONNECTED) {
            return;
        }

        Collection<Member> memberList = client.getClientClusterService().getMemberList();
        Collection<ConnectionAttempt> connectionAttempts = new LinkedList<>();
        for (Member member : memberList) {
            UUID uuid = member.getUuid();
            if (activeConnections.get(uuid) != null) {
                continue;
            }
            Address address = translate(member);
            logger.info("Trying to open connection to member at " + address);
            TcpClientConnection connection = createSocketConnection(address);
            Future<ClientAuthenticationCodec.ResponseParameters> future = authenticateAsync(connection)
                    .thenApply(clientMessage -> {
                        ClientAuthenticationCodec.ResponseParameters response =
                                ClientAuthenticationCodec.decodeResponse(clientMessage);
                        checkAuthenticationResponse(connection, response);
                        connection.setConnectedServerVersion(response.serverHazelcastVersion);
                        connection.setRemoteAddress(response.address);
                        connection.setRemoteUuid(response.memberUuid);
                        return response;
                    });
            connectionAttempts.add(new ConnectionAttempt(member.getUuid(), future, connection));
        }

        for (ConnectionAttempt attempt : connectionAttempts) {
            try {
                // This can potentially block more than authentication timeout.
                onAuthenticatedToMember(attempt.connection, attempt.future.get(authenticationTimeout, TimeUnit.MILLISECONDS));
            } catch (Exception e) {
                if (e instanceof TimeoutException) {
                    attempt.connection.close("Authentication timeout", e);
                }
                if (logger.isFineEnabled()) {
                    logger.warning("Could not connect to member " + attempt.memberUuid, e);
                } else {
                    logger.warning("Could not connect to member " + attempt.memberUuid + ", reason " + e);
                }
            }
        }
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {

    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        Connection connection = getConnection(member.getUuid());
        if (connection != null) {
            connection.close(null,
                    new TargetDisconnectedException("The client has closed the connection to this member,"
                            + " after receiving a member left event from the cluster. " + connection));
        }
    }
}

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
import com.hazelcast.client.impl.spi.ClientMemberListProvider;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.sql.impl.QueryUtils;

import javax.annotation.Nonnull;
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
import java.util.concurrent.CompletableFuture;
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
import static com.hazelcast.client.impl.protocol.AuthenticationStatus.NOT_ALLOWED_IN_CLUSTER;
import static com.hazelcast.client.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static com.hazelcast.client.properties.ClientProperty.SHUFFLE_MEMBER_LIST;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CHANGED_CLUSTER;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
public class TcpClientConnectionManager implements ClientConnectionManager, MembershipListener {

    private static final int SQL_CONNECTION_RANDOM_ATTEMPTS = 10;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();
    protected final LifecycleServiceImpl lifecycleService;
    protected final LoggingService loggingService;

    private final AtomicBoolean isAlive = new AtomicBoolean();
    private final ILogger logger;
    private final int connectionTimeoutMillis;
    private final Collection<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();
    private final Networking networking;
    private final ClientMemberListProvider memberListProvider;
    private final HazelcastClientInstanceImpl client;
    private final Authenticator authenticator;
    private final long authenticationTimeout;
    // outboundPorts is accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<>();
    private final int outboundPortCount;
    private final ExecutorService clusterExecutor;
    private final boolean shuffleMemberList;
    private final WaitStrategy waitStrategy;
    private final ClusterDiscoveryService clusterDiscoveryService;
    private final boolean asyncStart;
    private final ReconnectMode reconnectMode;
    private final LoadBalancer loadBalancer;
    private final boolean isSmartRoutingEnabled;
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final ClientConfig clientConfig;
    private final String clientName;
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

    @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:ExecutableStatementCount"})
    public TcpClientConnectionManager(LoggingService loggingService, ClientConfig clientConfig,
                                      HazelcastProperties properties,
                                      ClusterDiscoveryService clusterDiscoveryService, String clientName,
                                      Networking networking, LifecycleServiceImpl lifecycleService,
                                      ClientMemberListProvider memberListProvider,
                                      HazelcastClientInstanceImpl client,
                                      Authenticator authenticator) {
        this.loggingService = loggingService;
        this.logger = loggingService.getLogger(TcpClientConnectionManager.class);
        this.clientConfig = clientConfig;
        this.clusterDiscoveryService = clusterDiscoveryService;
        this.clientName = clientName;
        this.networking = networking;
        this.lifecycleService = lifecycleService;
        this.memberListProvider = memberListProvider;
        this.client = client;
        this.authenticator = authenticator;
        this.loadBalancer = createLoadBalancer(clientConfig);
        this.connectionTimeoutMillis = initConnectionTimeoutMillis();
        this.outboundPorts.addAll(getOutboundPorts());
        this.outboundPortCount = outboundPorts.size();
        this.authenticationTimeout = properties.getPositiveMillisOrDefault(HEARTBEAT_TIMEOUT);
        this.clusterExecutor = createClusterExecutorService();
        this.waitStrategy = initializeWaitStrategy(clientConfig);
        this.shuffleMemberList = properties.getBoolean(SHUFFLE_MEMBER_LIST);
        this.isSmartRoutingEnabled = clientConfig.getNetworkConfig().isSmartRouting();
        this.asyncStart = clientConfig.getConnectionStrategyConfig().isAsyncStart();
        this.reconnectMode = clientConfig.getConnectionStrategyConfig().getReconnectMode();
    }

    private static LoadBalancer createLoadBalancer(ClientConfig config) {
        LoadBalancer lb = config.getLoadBalancer();
        if (lb == null) {
            if (config.getLoadBalancerClassName() != null) {
                try {
                    return ClassLoaderUtil.newInstance(config.getClassLoader(), config.getLoadBalancerClassName());
                } catch (Exception e) {
                    rethrow(e);
                }
            } else {
                lb = new RoundRobinLB();
            }
        }
        return lb;
    }

    private int initConnectionTimeoutMillis() {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        final int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
    }

    private ExecutorService createClusterExecutorService() {
        ClassLoader classLoader = clientConfig.getClassLoader();
        SingleExecutorThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, clientName + ".cluster");
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    private Collection<Integer> getOutboundPorts() {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        Collection<Integer> outboundPorts = networkConfig.getOutboundPorts();
        Collection<String> outboundPortDefinitions = networkConfig.getOutboundPortDefinitions();
        return AddressUtil.getOutboundPorts(outboundPorts, outboundPortDefinitions);
    }

    public LoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    public Networking getNetworking() {
        return networking;
    }

    private WaitStrategy initializeWaitStrategy(ClientConfig clientConfig) {
        ConnectionRetryConfig retryConfig = clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig();

        long clusterConnectTimeout = retryConfig.getClusterConnectTimeoutMillis();

        if (clusterConnectTimeout == DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS) {
            // If no value is provided, or set to -1 explicitly,
            // use a predefined timeout value for the failover client
            // and infinite for the normal client.
            if (clusterDiscoveryService.failoverEnabled()) {
                clusterConnectTimeout = FAILOVER_CLIENT_DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS;
            } else {
                clusterConnectTimeout = Long.MAX_VALUE;
            }
        }

        return new WaitStrategy(retryConfig.getInitialBackoffMillis(), retryConfig.getMaxBackoffMillis(),
                retryConfig.getMultiplier(), clusterConnectTimeout, retryConfig.getJitter(), logger);
    }

    public synchronized void start() {
        if (!isAlive.compareAndSet(false, true)) {
            return;
        }
        startNetworking();

        clusterDiscoveryService.current().start();
        if (asyncStart) {
            asyncConnectToCluster();
        } else {
            connectToCluster();
        }
    }

    public void tryConnectToAllClusterMembers() {
        if (isSmartRoutingEnabled && !asyncStart) {
            connectToAllClusterMembers();
        }
    }

    public void startClusterThread() {
        clusterExecutor.execute(this::clusterThreadLoop);
    }

    void consumeTaskQueue() {
        Runnable runnable = taskQueue.poll();
        while (runnable != null) {
            runnable.run();
            runnable = taskQueue.poll();
        }
    }

    private void clusterThreadLoop() {
        while (isAlive.get()) {
            try {
                consumeTaskQueue();
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
        connectionClosed(connection);
        if (tryRemoveFromActiveConnections(connection)) {
            // already removed from active connections, no need to take further action
            return;
        }

        if (!activeConnections.isEmpty()) {
            //if there are still active connections, we don't have to do anything
            return;
        }

        // if there are no more connections, we are disconnected only if we were connected to a cluster
        if (clientState == ClientState.CONNECTED) {
            lifecycleService.fireLifecycleEvent(LifecycleState.CLIENT_DISCONNECTED);
        }

        clientState = ClientState.DISCONNECTED;

        if (reconnectMode == OFF) {
            logger.info("RECONNECT MODE is off. Shutting down the client.");
            shutdownWithExternalThread();
            return;
        }

        retryConnectToCluster();
    }

    private boolean tryRemoveFromActiveConnections(TcpClientConnection connection) {
        Address endpoint = connection.getRemoteAddress();
        UUID memberUuid = connection.getRemoteUuid();
        if (memberUuid == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying " + connection + ", but it has remote uuid set to null "
                        + "-> not removing it from a connection map");
            }
            return true;
        }

        if (activeConnections.remove(memberUuid, connection)) {
            logger.info("Removed connection to endpoint: " + endpoint + ":" + memberUuid + ", connection: " + connection);

            fireConnectionEvent(connection, false);
        } else if (logger.isFinestEnabled()) {
            logger.finest("Destroying a connection, but there is no mapping " + endpoint + ":" + memberUuid
                    + " -> " + connection + " in the connection map.");
        }
        return false;
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

    private void asyncConnectToCluster() {
        taskQueue.add(this::retryConnectToCluster);
    }

    private void retryConnectToCluster() {
        if (!lifecycleService.isRunning()) {
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
        String msg = lifecycleService.isRunning() ? "Unable to connect to any cluster." : "Client is being shutdown.";
        throw new IllegalStateException(msg);
    }

    private boolean checkClusterId(AuthenticationResult result) {
        if (clusterDiscoveryService.failoverEnabled() && isClusterIdChanged(result.response.clusterId)) {
            // If failover is provided, and this single connection is established to a different cluster, while
            // we were trying to connect back to the same cluster, we should failover to the next cluster.
            // Otherwise, we force the failover logic to be used by throwing `ClientNotAllowedInClusterException`
            result.connection.close("Connection belongs to another cluster. " + "Current cluster id"
                    + clusterId + ", new cluster id " + result.response.clusterId
                    + "Closing this to open a connection to next cluster with the correct client config", null);
            return false;
        }
        return true;
    }

    private boolean destroyCurrentClusterConnectionAndTryNextCluster(CandidateClusterContext currentContext,
                                                                     CandidateClusterContext nextContext) {
        currentContext.destroy();
        onClusterChange(nextContext.getClusterName());
        nextContext.start();

        logger.info("Trying to connect to next cluster: " + nextContext.getClusterName());
        AuthenticationResult result = doConnectToCandidateCluster(nextContext);
        if (result != null && onAuthenticatedToCluster(result)) {
            memberListProvider.waitInitialMemberListFetched();
            lifecycleService.fireLifecycleEvent(CLIENT_CHANGED_CLUSTER);
            return true;
        }
        return false;
    }


    private AuthenticationResult doConnectToCandidateCluster(CandidateClusterContext context) {
        Set<Address> triedAddresses = new HashSet<>();
        try {
            waitStrategy.reset();
            do {
                Set<Address> triedAddressesPerAttempt = new HashSet<>();

                List<Member> memberList = new ArrayList<>(memberListProvider.getMemberList());
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
            logger.warning("Stopped trying on the cluster: " + context.getClusterName() + " reason: " + e.getMessage());
        }

        logger.info("Unable to connect to any address from the cluster with name: " + context.getClusterName()
                + ". The following addresses were tried: " + triedAddresses);
        return null;
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
                lifecycleService.shutdown();
            } catch (Exception exception) {
                logger.severe("Exception during client shutdown", exception);
            }
        }, clientName + ".clientShutdown-").start();
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
            Map attributeMap = channel.attributeMap();
            attributeMap.put(Address.class, target);

            InetSocketAddress inetSocketAddress = new InetSocketAddress(target.getInetAddress(), target.getPort());
            channel.connect(inetSocketAddress, connectionTimeoutMillis);


            socketChannel.configureBlocking(true);
            SocketInterceptor socketInterceptor = currentClusterContext.getSocketInterceptor();
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }

            TcpClientConnection connection = new TcpClientConnection(this, lifecycleService,
                    loggingService, connectionIdGen.incrementAndGet(), channel);
            attributeMap.put(TcpClientConnection.class, connection);
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
                Member member = QueryUtils.memberOfLargerSameVersionGroup(memberListProvider.getMemberList(), null);
                if (member == null) {
                    break;
                }
                ClientConnection connection = activeConnections.get(member.getUuid());
                if (connection != null) {
                    return connection;
                }
            }
        }

        // Otherwise, iterate over connections and return the first one that's not to a lite member
        ClientConnection firstConnection = null;
        for (Map.Entry<UUID, TcpClientConnection> connectionEntry : activeConnections.entrySet()) {
            if (firstConnection == null) {
                firstConnection = connectionEntry.getValue();
            }
            UUID memberId = connectionEntry.getKey();
            Member member = memberListProvider.getMember(memberId);
            if (member == null || member.isLiteMember()) {
                continue;
            }
            return connectionEntry.getValue();
        }

        // Failed to get a connection to a data member
        return firstConnection;
    }

    private ClientAuthenticationCodec.ResponseParameters authenticate(TcpClientConnection connection) {
        CompletableFuture<ClientMessage> future = authenticator.authenticate(connection);
        try {
            return ClientAuthenticationCodec.decodeResponse(future.get(authenticationTimeout, MILLISECONDS));
        } catch (Exception e) {
            connection.close("Failed to authenticate connection", e);
            throw rethrow(e);
        }
    }

    private void onAuthenticatedToMember(TcpClientConnection connection, ClientAuthenticationCodec.ResponseParameters response) {
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
            onClusterConnect();
        }
        clusterId = newClusterId;
        connectionAuthenticated(connection, response);
        if (clusterIdChanged) {
            try {
                sendStateToCluster();
                if (logger.isFineEnabled()) {
                    logger.fine("Client state is sent to cluster: " + clusterId);
                }
            } catch (RuntimeException e) {
                logger.warning("Failure during sending state to the cluster. " + connection, e);
                tryRemoveFromActiveConnections(connection);
                connection.close("Failure during sending state to the cluster.", null);
                return false;
            }
        }
        clientState = ClientState.CONNECTED;
        lifecycleService.fireLifecycleEvent(LifecycleState.CLIENT_CONNECTED);
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

    private void connectionAuthenticated(TcpClientConnection connection,
                                         ClientAuthenticationCodec.ResponseParameters response) {
        activeConnections.put(connection.getRemoteUuid(), connection);
        logger.info("Authenticated with server " + response.address + ":" + response.memberUuid + ", server version: "
                + response.serverHazelcastVersion + ", local address: " + connection.getLocalSocketAddress());
        fireConnectionEvent(connection, true);
    }

    /**
     * Checks the response from the server to see if authentication needs to be continued,
     * closes the connection and throws exception if the authentication needs to be cancelled.
     */
    private void checkAuthenticationResponse(TcpClientConnection connection,
                                             ClientAuthenticationCodec.ResponseParameters response) {
        AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(response.status);
        if (clusterDiscoveryService.failoverEnabled() && !response.failoverSupported) {
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
                ClientNotAllowedInClusterException notAllowedException
                        = new ClientNotAllowedInClusterException("Client is not allowed in the cluster");
                connection.close("Failed to authenticate connection", notAllowedException);
                throw notAllowedException;
            default:
                AuthenticationException exception =
                        new AuthenticationException("Authentication status code not supported. status: " + authenticationStatus);
                connection.close("Failed to authenticate connection", exception);
                throw exception;
        }
        int appliedPartitionCount = getAndSetPartitionCount(response.partitionCount);
        if (appliedPartitionCount != response.partitionCount) {
            ClientNotAllowedInClusterException exception =
                    new ClientNotAllowedInClusterException("Client can not work with this cluster"
                            + " because it has a different partition count. "
                            + "Expected partition count: " + appliedPartitionCount
                            + ", Member partition count: " + response.partitionCount);
            connection.close("Failed to authenticate connection", exception);
            throw exception;
        }
    }


    protected void checkClientActive() {
        if (!lifecycleService.isRunning()) {
            throw new HazelcastClientNotActiveException();
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

    void connectToAllClusterMembers() {
        if (!lifecycleService.isRunning()) {
            return;
        }

        if (clientState != ClientState.CONNECTED) {
            return;
        }

        Collection<Member> memberList = memberListProvider.getMemberList();
        Collection<ConnectionAttempt> connectionAttempts = new LinkedList<>();
        for (Member member : memberList) {
            UUID uuid = member.getUuid();
            if (activeConnections.get(uuid) != null) {
                continue;
            }
            Address address = translate(member);
            logger.info("Trying to open connection to member at " + address);
            TcpClientConnection connection = createSocketConnection(address);
            Future<ClientAuthenticationCodec.ResponseParameters> future = authenticator.authenticate(connection)
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
                // We would like to print the stack trace of the exception only if fine is enabled.
                // Otherwise, we print just the message(toString) of the exception.
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

    protected void connectionClosed(TcpClientConnection connection) {
        client.onConnectionClose(connection);
    }

    protected void onClusterChange(String newClusterName) {
        client.onClusterChange(newClusterName);
    }

    protected void onClusterConnect() {
        client.onClusterConnect();
    }

    protected int getAndSetPartitionCount(int partitionCount) {
        return client.getAndSetPartitionCount(partitionCount);
    }

    protected void sendStateToCluster() {
        client.sendStateToCluster();
    }

}

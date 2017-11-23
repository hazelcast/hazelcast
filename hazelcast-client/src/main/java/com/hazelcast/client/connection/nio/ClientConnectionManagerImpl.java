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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.ClientConnectionStrategy;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.LifecycleServiceImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.Member;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.client.config.SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.client.spi.properties.ClientProperty.SHUFFLE_MEMBER_LIST;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_CLIENT_BUFFER_DIRECT;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientConnectionManagerImpl implements ClientConnectionManager, ConnectionHeartbeatListener {

    private static final int DEFAULT_SSL_THREAD_COUNT = 3;
    private static final int DEFAULT_CONNECTION_ATTEMPT_LIMIT_SYNC = 2;
    private static final int DEFAULT_CONNECTION_ATTEMPT_LIMIT_ASYNC = 20;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    protected volatile boolean alive;


    private final ILogger logger;
    private final int connectionTimeout;

    private final HazelcastClientInstanceImpl client;
    private final SocketInterceptor socketInterceptor;
    private final SocketOptions socketOptions;
    private final ChannelFactory channelFactory;

    private final ClientExecutionServiceImpl executionService;
    private final AddressTranslator addressTranslator;
    private final ConcurrentMap<Address, ClientConnection> activeConnections
            = new ConcurrentHashMap<Address, ClientConnection>();
    private final ConcurrentMap<Address, AuthenticationFuture> connectionsInProgress =
            new ConcurrentHashMap<Address, AuthenticationFuture>();
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Credentials credentials;
    private final AtomicLong correlationIddOfLastAuthentication = new AtomicLong(0);
    private final NioEventLoopGroup eventLoopGroup;

    private volatile Address ownerConnectionAddress;

    private HeartbeatManager heartbeat;
    private volatile ClientPrincipal principal;
    private final ClientConnectionStrategy connectionStrategy;
    private final ExecutorService clusterConnectionExecutor;
    private final int connectionAttemptPeriod;
    private final int connectionAttemptLimit;
    private final boolean shuffleMemberList;
    private final Collection<AddressProvider> addressProviders;
    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();
    private final int outboundPortCount;

    @SuppressWarnings("checkstyle:executablestatementcount")
    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator,
                                       Collection<AddressProvider> addressProviders) {

        this.client = client;
        this.addressTranslator = addressTranslator;

        this.logger = client.getLoggingService().getLogger(ClientConnectionManager.class);

        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        this.connectionTimeout = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.socketOptions = networkConfig.getSocketOptions();

        this.eventLoopGroup = initEventLoopGroup(client);

        this.channelFactory = client.getClientExtension().createSocketChannelWrapperFactory();
        this.socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());

        this.credentials = client.getCredentials();
        this.connectionStrategy = initializeStrategy(client);
        this.clusterConnectionExecutor = createSingleThreadExecutorService(client);
        this.shuffleMemberList = client.getProperties().getBoolean(SHUFFLE_MEMBER_LIST);
        this.addressProviders = addressProviders;
        connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();

        int connAttemptLimit = networkConfig.getConnectionAttemptLimit();
        boolean isAsync = client.getClientConfig().getConnectionStrategyConfig().isAsyncStart();

        if (connAttemptLimit < 0) {
            this.connectionAttemptLimit = isAsync ? DEFAULT_CONNECTION_ATTEMPT_LIMIT_ASYNC
                    : DEFAULT_CONNECTION_ATTEMPT_LIMIT_SYNC;
        } else {
            this.connectionAttemptLimit = connAttemptLimit == 0 ? Integer.MAX_VALUE : connAttemptLimit;
        }

        this.outboundPorts.addAll(getOutboundPorts(networkConfig));
        this.outboundPortCount = outboundPorts.size();

        checkSslAllowed();
    }

    private void checkSslAllowed() {
        SSLConfig sslConfig = client.getClientConfig().getNetworkConfig().getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("SSL/TLS requires Hazelcast Enterprise Edition");
            }
        }
    }

    private Collection<Integer> getOutboundPorts(ClientNetworkConfig networkConfig) {
        Collection<Integer> outboundPorts = networkConfig.getOutboundPorts();
        Collection<String> outboundPortDefinitions = networkConfig.getOutboundPortDefinitions();
        return AddressUtil.getOutboundPorts(outboundPorts, outboundPortDefinitions);
    }

    private ClientConnectionStrategy initializeStrategy(HazelcastClientInstanceImpl client) {
        ClientConnectionStrategy strategy;
        //internal property
        String className = client.getProperties().get("hazelcast.client.connection.strategy.classname");
        if (className != null) {
            try {
                ClassLoader configClassLoader = client.getClientConfig().getClassLoader();
                return ClassLoaderUtil.newInstance(configClassLoader, className);
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            strategy = new DefaultClientConnectionStrategy();
        }
        return strategy;
    }

    public NioEventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    protected NioEventLoopGroup initEventLoopGroup(HazelcastClientInstanceImpl client) {
        HazelcastProperties properties = client.getProperties();
        boolean directBuffer = properties.getBoolean(SOCKET_CLIENT_BUFFER_DIRECT);

        SSLConfig sslConfig = client.getClientConfig().getNetworkConfig().getSSLConfig();
        boolean sslEnabled = sslConfig != null && sslConfig.isEnabled();

        int configuredInputThreads = properties.getInteger(ClientProperty.IO_INPUT_THREAD_COUNT);
        int configuredOutputThreads = properties.getInteger(ClientProperty.IO_OUTPUT_THREAD_COUNT);

        int inputThreads;
        if (configuredInputThreads == -1) {
            inputThreads = sslEnabled ? DEFAULT_SSL_THREAD_COUNT : 1;
        } else {
            inputThreads = configuredInputThreads;
        }

        int outputThreads;
        if (configuredOutputThreads == -1) {
            outputThreads = sslEnabled ? DEFAULT_SSL_THREAD_COUNT : 1;
        } else {
            outputThreads = configuredOutputThreads;
        }

        return new NioEventLoopGroup(
                client.getLoggingService(),
                client.getMetricsRegistry(),
                client.getName(),
                new ClientConnectionChannelErrorHandler(),
                inputThreads,
                outputThreads,
                properties.getInteger(ClientProperty.IO_BALANCER_INTERVAL_SECONDS),
                new ClientChannelInitializer(getBufferSize(), directBuffer));
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            ClientExtension clientExtension = client.getClientExtension();
            return clientExtension.createSocketInterceptor();
        }
        return null;
    }

    @Override
    public Collection<ClientConnection> getActiveConnections() {
        return activeConnections.values();
    }

    @Override
    public boolean isAlive() {
        return alive;
    }

    public synchronized void start(ClientContext clientContext) throws Exception {
        if (alive) {
            return;
        }
        alive = true;
        startEventLoopGroup();
        heartbeat = new HeartbeatManager(this, client);
        heartbeat.start();
        addConnectionHeartbeatListener(this);
        connectionStrategy.init(clientContext);
        connectionStrategy.start();
    }

    protected void startEventLoopGroup() {
        eventLoopGroup.start();
    }

    public synchronized void shutdown() {
        if (!alive) {
            return;
        }
        alive = false;

        for (Connection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }
        ClientExecutionServiceImpl.shutdownExecutor("cluster", clusterConnectionExecutor, logger);
        stopEventLoopGroup();
        connectionListeners.clear();
        heartbeat.shutdown();

        connectionStrategy.shutdown();
    }

    @Override
    public ClientPrincipal getPrincipal() {
        return principal;
    }

    private void setPrincipal(ClientPrincipal principal) {
        this.principal = principal;
    }

    protected void stopEventLoopGroup() {
        eventLoopGroup.shutdown();
    }

    @Override
    public Connection getActiveConnection(Address target) {
        if (target == null) {
            return null;
        }
        return activeConnections.get(target);
    }

    @Override
    public Connection getOrConnect(Address address) throws IOException {
        return getOrConnect(address, false);
    }

    @Override
    public Connection getOrTriggerConnect(Address target) throws IOException {
        Connection connection = getConnection(target, false);
        if (connection != null) {
            return connection;
        }
        triggerConnect(target, false);
        return null;
    }

    private Connection getConnection(Address target, boolean asOwner) throws IOException {
        if (!asOwner) {
            connectionStrategy.beforeGetConnection(target);
        }
        if (!asOwner && getOwnerConnection() == null) {
            throw new IOException("Owner connection is not available!");
        }
        if (target == null) {
            throw new IllegalStateException("Address can not be null");
        }

        ClientConnection connection = activeConnections.get(target);

        if (connection != null) {
            if (!asOwner) {
                return connection;
            }
            if (connection.isAuthenticatedAsOwner()) {
                return connection;
            }
        }
        return null;
    }

    @Override
    public Address getOwnerConnectionAddress() {
        return ownerConnectionAddress;
    }

    private void setOwnerConnectionAddress(Address ownerConnectionAddress) {
        this.ownerConnectionAddress = ownerConnectionAddress;
    }

    private Connection getOrConnect(Address address, boolean asOwner) throws IOException {
        try {
            while (true) {
                ClientConnection connection = (ClientConnection) getConnection(address, asOwner);
                if (connection != null) {
                    return connection;
                }
                AuthenticationFuture firstCallback = triggerConnect(address, asOwner);
                connection = (ClientConnection) firstCallback.get(connectionTimeout);

                if (!asOwner) {
                    return connection;
                }
                if (connection.isAuthenticatedAsOwner()) {
                    return connection;
                }
            }
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private AuthenticationFuture triggerConnect(Address target, boolean asOwner) {
        if (!asOwner) {
            connectionStrategy.beforeOpenConnection(target);
        }
        if (!alive) {
            throw new HazelcastException("ConnectionManager is not active!");
        }

        AuthenticationFuture callback = new AuthenticationFuture();
        AuthenticationFuture firstCallback = connectionsInProgress.putIfAbsent(target, callback);
        if (firstCallback == null) {
            executionService.execute(new InitConnectionTask(target, asOwner, callback));
            return callback;
        }
        return firstCallback;
    }

    @Override
    public ClientConnection getOwnerConnection() {
        if (ownerConnectionAddress == null) {
            return null;
        }
        ClientConnection connection = (ClientConnection) getActiveConnection(ownerConnectionAddress);
        return connection;
    }

    private Connection connectAsOwner(Address address) {
        Connection connection = null;
        try {
            logger.info("Trying to connect to " + address + " as owner member");
            connection = getOrConnect(address, true);
            client.onClusterConnect(connection);
            setOwnerConnectionAddress(connection.getEndPoint());
            logger.info("Setting " + connection + " as owner with principal " + principal);
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

    private void fireConnectionAddedEvent(ClientConnection connection) {
        for (ConnectionListener connectionListener : connectionListeners) {
            connectionListener.connectionAdded(connection);
        }
        connectionStrategy.onConnect(connection);
    }

    private void fireConnectionRemovedEvent(ClientConnection connection) {
        if (connection.isAuthenticatedAsOwner()) {
            disconnectFromCluster(connection);
        }

        for (ConnectionListener listener : connectionListeners) {
            listener.connectionRemoved(connection);
        }
        connectionStrategy.onDisconnect(connection);
    }

    private void disconnectFromCluster(final ClientConnection connection) {
        clusterConnectionExecutor.execute(new Runnable() {
            @Override
            public void run() {
                Address endpoint = connection.getEndPoint();
                /**
                 * it may be possible that while waiting on executor queue, the client got connected (another connection),
                 * then we do not need to do anything for cluster disconnect.
                 */
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

    protected ClientConnection createSocketConnection(final Address address) throws IOException {
        if (!alive) {
            throw new HazelcastException("ConnectionManager is not active!");
        }
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();
            socket.setKeepAlive(socketOptions.isKeepAlive());
            socket.setTcpNoDelay(socketOptions.isTcpNoDelay());
            socket.setReuseAddress(socketOptions.isReuseAddress());
            if (socketOptions.getLingerSeconds() > 0) {
                socket.setSoLinger(true, socketOptions.getLingerSeconds());
            }
            int bufferSize = getBufferSize();
            socket.setSendBufferSize(bufferSize);
            socket.setReceiveBufferSize(bufferSize);
            InetSocketAddress inetSocketAddress = address.getInetSocketAddress();
            bindSocketToPort(socket);
            socketChannel.socket().connect(inetSocketAddress, connectionTimeout);

            HazelcastProperties properties = client.getProperties();
            boolean directBuffer = properties.getBoolean(SOCKET_CLIENT_BUFFER_DIRECT);

            Channel channel = channelFactory.create(socketChannel, true, directBuffer);

            final ClientConnection clientConnection = new ClientConnection(
                    client, connectionIdGen.incrementAndGet(), channel);
            socketChannel.configureBlocking(true);
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }
            socket.setSoTimeout(0);

            eventLoopGroup.register(channel);
            return clientConnection;
        } catch (Exception e) {
            if (socketChannel != null) {
                socketChannel.close();
            }
            throw rethrow(e, IOException.class);
        }
    }

    private int getBufferSize() {
        int bufferSize = socketOptions.getBufferSize() * KILO_BYTE;
        return bufferSize <= 0 ? DEFAULT_BUFFER_SIZE_BYTE : bufferSize;
    }

    void onClose(Connection connection) {
        removeFromActiveConnections(connection);
    }

    private void removeFromActiveConnections(Connection connection) {
        Address endpoint = connection.getEndPoint();

        if (endpoint == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying " + connection + ", but it has end-point set to null "
                        + "-> not removing it from a connection map");
            }
            return;
        }
        if (activeConnections.remove(endpoint, connection)) {
            logger.info("Removed connection to endpoint: " + endpoint + ", connection: " + connection);
            fireConnectionRemovedEvent((ClientConnection) connection);
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

    @Override
    public void addConnectionHeartbeatListener(ConnectionHeartbeatListener connectionHeartbeatListener) {
        heartbeat.addConnectionHeartbeatListener(connectionHeartbeatListener);
    }

    private void authenticate(final Address target, final ClientConnection connection, final boolean asOwner,
                              final AuthenticationFuture callback) {
        final ClientPrincipal principal = getPrincipal();
        ClientMessage clientMessage = encodeAuthenticationRequest(asOwner, client.getSerializationService(), principal);
        ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, null, connection);
        ClientInvocationFuture future = clientInvocation.invokeUrgent();
        if (asOwner && clientInvocation.getSendConnection() != null) {
            correlationIddOfLastAuthentication.set(clientInvocation.getClientMessage().getCorrelationId());
        }
        future.andThen(new ExecutionCallback<ClientMessage>() {
            @Override
            public void onResponse(ClientMessage response) {
                ClientAuthenticationCodec.ResponseParameters result;
                try {
                    result = ClientAuthenticationCodec.decodeResponse(response);
                } catch (HazelcastException e) {
                    onFailure(e);
                    return;
                }
                AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(result.status);
                switch (authenticationStatus) {
                    case AUTHENTICATED:
                        connection.setConnectedServerVersion(result.serverHazelcastVersion);
                        connection.setRemoteEndpoint(result.address);
                        if (asOwner) {
                            if (!(correlationIddOfLastAuthentication.get() == response.getCorrelationId())) {
                                //if not same, client already gave up on this and send another authentication.
                                onFailure(new AuthenticationException("Owner authentication response from address "
                                        + target + " is late. Dropping the response. Principal: " + principal));
                                return;
                            }
                            connection.setIsAuthenticatedAsOwner();
                            ClientPrincipal principal = new ClientPrincipal(result.uuid, result.ownerUuid);
                            setPrincipal(principal);
                        }
                        onAuthenticated(target, connection);
                        callback.onSuccess(connection);
                        break;
                    case CREDENTIALS_FAILED:
                        onFailure(new AuthenticationException("Invalid credentials! Principal: " + principal));
                        break;
                    default:
                        onFailure(new AuthenticationException("Authentication status code not supported. status: "
                                + authenticationStatus));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                onAuthenticationFailed(target, connection, t);
                callback.onFailure(t);
            }
        });
    }

    private ClientMessage encodeAuthenticationRequest(boolean asOwner, SerializationService ss, ClientPrincipal principal) {
        byte serializationVersion = ((InternalSerializationService) ss).getVersion();
        String uuid = null;
        String ownerUuid = null;
        if (principal != null) {
            uuid = principal.getUuid();
            ownerUuid = principal.getOwnerUuid();
        }
        ClientMessage clientMessage;
        if (credentials.getClass().equals(UsernamePasswordCredentials.class)) {
            UsernamePasswordCredentials cr = (UsernamePasswordCredentials) credentials;
            clientMessage = ClientAuthenticationCodec
                    .encodeRequest(cr.getUsername(), cr.getPassword(), uuid, ownerUuid, asOwner, ClientTypes.JAVA,
                            serializationVersion, BuildInfoProvider.getBuildInfo().getVersion());
        } else {
            Data data = ss.toData(credentials);
            clientMessage = ClientAuthenticationCustomCodec.encodeRequest(data, uuid, ownerUuid,
                    asOwner, ClientTypes.JAVA, serializationVersion, BuildInfoProvider.getBuildInfo().getVersion());
        }
        return clientMessage;
    }

    private void onAuthenticated(Address target, ClientConnection connection) {
        ClientConnection oldConnection = activeConnections.put(connection.getEndPoint(), connection);
        if (oldConnection == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Authentication succeeded for " + connection
                        + " and there was no old connection to this end-point");
            }
            fireConnectionAddedEvent(connection);
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Re-authentication succeeded for " + connection);
            }
            assert connection.equals(oldConnection);
        }

        connectionsInProgress.remove(target);
        logger.info("Authenticated with server " + connection.getEndPoint() + ", server version:" + connection
                .getConnectedServerVersionString() + " Local address: " + connection.getLocalSocketAddress());

        /* check if connection is closed by remote before authentication complete, if that is the case
        we need to remove it back from active connections.
        Race description from https://github.com/hazelcast/hazelcast/pull/8832.(A little bit changed)
        - open a connection client -> member
        - send auth message
        - receive auth reply -> reply processing is offloaded to an executor. Did not start to run yet.
        - member closes the connection -> the connection is trying to removed from map
                                                             but it was not there to begin with
        - the executor start processing the auth reply -> it put the connection to the connection map.
        - we end up with a closed connection in activeConnections map */
        if (!connection.isAlive()) {
            removeFromActiveConnections(connection);
        }
    }

    private void onAuthenticationFailed(Address target, ClientConnection connection, Throwable cause) {
        if (logger.isFinestEnabled()) {
            logger.finest("Authentication of " + connection + " failed.", cause);
        }
        connection.close(null, cause);
        connectionsInProgress.remove(target);
    }

    @Override
    public void heartbeatResumed(Connection connection) {
        connectionStrategy.onHeartbeatResumed((ClientConnection) connection);
    }

    @Override
    public void heartbeatStopped(Connection connection) {
        connectionStrategy.onHeartbeatStopped((ClientConnection) connection);
    }

    private class InitConnectionTask implements Runnable {

        private final Address target;
        private final boolean asOwner;
        private final AuthenticationFuture callback;

        InitConnectionTask(Address target, boolean asOwner, AuthenticationFuture callback) {
            this.target = target;
            this.asOwner = asOwner;
            this.callback = callback;
        }

        @Override
        public void run() {
            ClientConnection connection;
            try {
                connection = getConnection(target);
            } catch (Exception e) {
                logger.finest(e);
                callback.onFailure(e);
                connectionsInProgress.remove(target);
                return;
            }

            try {
                authenticate(target, connection, asOwner, callback);
            } catch (Exception e) {
                callback.onFailure(e);
                connection.close("Failed to authenticate connection", e);
                connectionsInProgress.remove(target);
            }
        }

        private ClientConnection getConnection(Address target) throws IOException {
            ClientConnection connection = activeConnections.get(target);
            if (connection != null) {
                return connection;
            }
            Address address = addressTranslator.translate(target);
            if (address == null) {
                throw new NullPointerException("Address Translator " + addressTranslator.getClass()
                        + " could not translate address " + target);
            }
            return createSocketConnection(address);
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

                ClientConnection connection = (ClientConnection) channel.attributeMap().get(ClientConnection.class);
                if (cause instanceof EOFException) {
                    connection.close("Connection closed by the other side", cause);
                } else {
                    connection.close("Exception in " + connection + ", thread=" + Thread.currentThread().getName(), cause);
                }
            }
        }
    }

    @Override
    public void connectToCluster() {
        try {
            connectToClusterAsync().get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void connectToClusterInternal() {
        int attempt = 0;
        Set<Address> triedAddresses = new HashSet<Address>();

        while (attempt < connectionAttemptLimit) {
            attempt++;
            long nextTry = Clock.currentTimeMillis() + connectionAttemptPeriod;

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

            /**
             * If the address providers load no addresses (which seems to be possible), then the above loop is not entered
             * and the lifecycle check is missing, hence we need to repeat the same check at this point.
             */
            if (!client.getLifecycleService().isRunning()) {
                throw new IllegalStateException("Client is being shutdown.");
            }

            if (attempt < connectionAttemptLimit) {
                final long remainingTime = nextTry - Clock.currentTimeMillis();
                logger.warning(String.format("Unable to get alive cluster connection, try in %d ms later, attempt %d of %d.",
                        Math.max(0, remainingTime), attempt, connectionAttemptLimit));

                if (remainingTime > 0) {
                    try {
                        Thread.sleep(remainingTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } else {
                logger.warning(String.format("Unable to get alive cluster connection, attempt %d of %d.", attempt,
                        connectionAttemptLimit));
            }
        }
        throw new IllegalStateException(
                "Unable to connect to any address in the config!" + " The following addresses were tried: " + triedAddresses);
    }

    @Override
    public Future<Void> connectToClusterAsync() {
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

    private Collection<Address> getPossibleMemberAddresses() {
        final List<Address> addresses = new LinkedList<Address>();

        Collection<Member> memberList = client.getClientClusterService().getMemberList();
        for (Member member : memberList) {
            addresses.add(member.getAddress());
        }

        if (shuffleMemberList) {
            Collections.shuffle(addresses);
        }

        List<Address> providerAddresses = new ArrayList<Address>();
        for (AddressProvider addressProvider : addressProviders) {
            providerAddresses.addAll(addressProvider.loadAddresses());
        }

        if (shuffleMemberList) {
            Collections.shuffle(providerAddresses);
        }

        addresses.addAll(providerAddresses);

        return addresses;
    }

    private ExecutorService createSingleThreadExecutorService(HazelcastClientInstanceImpl client) {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        SingleExecutorThreadFactory threadFactory = new SingleExecutorThreadFactory(classLoader, client.getName() + ".cluster-");

        return Executors.newSingleThreadExecutor(threadFactory);
    }
}

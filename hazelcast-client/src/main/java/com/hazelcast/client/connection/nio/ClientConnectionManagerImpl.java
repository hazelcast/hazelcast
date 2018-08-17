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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.ClientConnectionStrategy;
import com.hazelcast.client.impl.ClientTypes;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.AddressUtil;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.spi.properties.ClientProperty.ALLOW_INVOCATIONS_WHEN_DISCONNECTED;
import static com.hazelcast.client.spi.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.client.spi.properties.ClientProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.client.spi.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final int DEFAULT_SSL_THREAD_COUNT = 3;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    protected volatile boolean alive;
    private final ILogger logger;
    private final int connectionTimeoutMillis;
    private final HazelcastClientInstanceImpl client;
    private final SocketInterceptor socketInterceptor;

    private final ClientExecutionServiceImpl executionService;
    private final AddressTranslator addressTranslator;
    private final ConcurrentMap<Address, ClientConnection> activeConnections = new ConcurrentHashMap<Address, ClientConnection>();
    private final ConcurrentMap<Address, AuthenticationFuture> connectionsInProgress =
            new ConcurrentHashMap<Address, AuthenticationFuture>();
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();
    private final boolean allowInvokeWhenDisconnected;
    private final ICredentialsFactory credentialsFactory;
    private final NioNetworking networking;
    private final HeartbeatManager heartbeat;
    private final ClusterConnector clusterConnector;
    private final long authenticationTimeout;
    private volatile ClientPrincipal principal;
    private final ClientConnectionStrategy connectionStrategy;
    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();
    private final int outboundPortCount;
    private volatile Credentials lastCredentials;

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator,
                                       Collection<AddressProvider> addressProviders) {
        allowInvokeWhenDisconnected = client.getProperties().getBoolean(ALLOW_INVOCATIONS_WHEN_DISCONNECTED);
        this.client = client;

        this.addressTranslator = addressTranslator;

        this.logger = client.getLoggingService().getLogger(ClientConnectionManager.class);

        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        this.connectionTimeoutMillis = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();

        this.networking = initNetworking(client);

        this.socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());

        this.credentialsFactory = client.getCredentialsFactory();
        this.connectionStrategy = initializeStrategy(client);

        this.outboundPorts.addAll(getOutboundPorts(networkConfig));
        this.outboundPortCount = outboundPorts.size();
        this.heartbeat = new HeartbeatManager(this, client);
        this.authenticationTimeout = heartbeat.getHeartbeatTimeout();

        this.clusterConnector = new ClusterConnector(client, this, connectionStrategy, addressProviders);
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

    public NioNetworking getNetworking() {
        return networking;
    }

    protected NioNetworking initNetworking(HazelcastClientInstanceImpl client) {
        HazelcastProperties properties = client.getProperties();

        SSLConfig sslConfig = client.getClientConfig().getNetworkConfig().getSSLConfig();
        boolean sslEnabled = sslConfig != null && sslConfig.isEnabled();

        int configuredInputThreads = properties.getInteger(IO_INPUT_THREAD_COUNT);
        int configuredOutputThreads = properties.getInteger(IO_OUTPUT_THREAD_COUNT);

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

        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(client.getLoggingService())
                        .metricsRegistry(client.getMetricsRegistry())
                        .threadNamePrefix(client.getName())
                        .errorHandler(new ClientConnectionChannelErrorHandler())
                        .inputThreadCount(inputThreads)
                        .outputThreadCount(outputThreads)
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .channelInitializer(client.getClientExtension().createChannelInitializer()));
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            ClientExtension clientExtension = client.getClientExtension();
            return clientExtension.createSocketInterceptor();
        }
        return null;
    }

    public ClientConnectionStrategy getConnectionStrategy() {
        return connectionStrategy;
    }

    @Override
    public Collection<ClientConnection> getActiveConnections() {
        return activeConnections.values();
    }

    @Override
    public boolean isAlive() {
        return alive;
    }

    public synchronized void start(ClientContext clientContext) {
        if (alive) {
            return;
        }
        alive = true;
        startNetworking();

        heartbeat.start();
        connectionStrategy.init(clientContext);
        connectionStrategy.start();
    }

    protected void startNetworking() {
        networking.start();
    }

    public synchronized void shutdown() {
        if (!alive) {
            return;
        }
        alive = false;

        for (Connection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }
        clusterConnector.shutdown();
        stopNetworking();
        connectionListeners.clear();
        heartbeat.shutdown();

        connectionStrategy.shutdown();
        credentialsFactory.destroy();
    }

    @Override
    public ClientPrincipal getPrincipal() {
        return principal;
    }

    private void setPrincipal(ClientPrincipal principal) {
        this.principal = principal;
    }

    protected void stopNetworking() {
        networking.shutdown();
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
    public Connection getOrTriggerConnect(Address target, boolean acquiresResources) throws IOException {
        Connection connection = getConnection(target, false, acquiresResources);
        if (connection != null) {
            return connection;
        }
        triggerConnect(target, false);
        return null;
    }

    private Connection getConnection(Address target, boolean asOwner, boolean acquiresResources) throws IOException {
        checkAllowed(target, asOwner, acquiresResources);
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

    private void checkAllowed(Address target, boolean asOwner, boolean acquiresResources) throws IOException {
        if (asOwner) {
            connectionStrategy.beforeConnectToCluster(target);
            //opening an owner connection is always allowed
            return;
        }
        try {
            connectionStrategy.beforeGetConnection(target);
        } catch (HazelcastClientOfflineException e) {
            if (allowInvokeWhenDisconnected && !acquiresResources) {
                //invocations that does not acquire resources are allowed to invoke when disconnected
                return;
            }
            throw e;
        }
        if (getOwnerConnection() == null) {
            if (allowInvokeWhenDisconnected && !acquiresResources) {
                //invocations that does not acquire resources are allowed to invoke when disconnected
                return;
            }
            throw new IOException("Owner connection is not available!");
        }
    }

    @Override
    public Address getOwnerConnectionAddress() {
        return clusterConnector.getOwnerConnectionAddress();
    }

    Connection getOrConnect(Address address, boolean asOwner) {
        try {
            while (true) {
                ClientConnection connection = (ClientConnection) getConnection(address, asOwner, true);
                if (connection != null) {
                    return connection;
                }
                AuthenticationFuture future = triggerConnect(address, asOwner);
                connection = (ClientConnection) future.get();

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

        AuthenticationFuture future = new AuthenticationFuture();
        AuthenticationFuture oldFuture = connectionsInProgress.putIfAbsent(target, future);
        if (oldFuture == null) {
            executionService.execute(new InitConnectionTask(target, asOwner, future));
            return future;
        }
        return oldFuture;
    }

    @Override
    public ClientConnection getOwnerConnection() {
        Address ownerConnectionAddress = clusterConnector.getOwnerConnectionAddress();
        if (ownerConnectionAddress == null) {
            return null;
        }
        return (ClientConnection) getActiveConnection(ownerConnectionAddress);
    }

    private void fireConnectionAddedEvent(ClientConnection connection) {
        for (ConnectionListener connectionListener : connectionListeners) {
            connectionListener.connectionAdded(connection);
        }
        connectionStrategy.onConnect(connection);
    }

    private void fireConnectionRemovedEvent(ClientConnection connection) {
        if (connection.isAuthenticatedAsOwner()) {
            clusterConnector.disconnectFromCluster(connection);
        }

        for (ConnectionListener listener : connectionListeners) {
            listener.connectionRemoved(connection);
        }
        connectionStrategy.onDisconnect(connection);
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

    protected ClientConnection createSocketConnection(final Address remoteAddress) throws IOException {
        if (!alive) {
            throw new HazelcastException("ConnectionManager is not active!");
        }
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();

            bindSocketToPort(socket);

            Channel channel = networking.register(socketChannel, true);
            channel.connect(remoteAddress.getInetSocketAddress(), connectionTimeoutMillis);

            ClientConnection connection
                    = new ClientConnection(client, connectionIdGen.incrementAndGet(), channel);

            socketChannel.configureBlocking(true);
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }

            channel.start();
            return connection;
        } catch (Exception e) {
            closeResource(socketChannel);
            throw rethrow(e, IOException.class);
        }
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

    private void authenticate(final Address target, final ClientConnection connection, final boolean asOwner,
                              final AuthenticationFuture future) {
        final ClientPrincipal principal = getPrincipal();
        ClientMessage clientMessage = encodeAuthenticationRequest(asOwner, client.getSerializationService(), principal);
        ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, null, connection);
        ClientInvocationFuture invocationFuture = clientInvocation.invokeUrgent();

        ScheduledFuture timeoutTaskFuture = executionService.schedule(
                new TimeoutAuthenticationTask(invocationFuture), authenticationTimeout, MILLISECONDS);
        invocationFuture.andThen(new AuthCallback(connection, asOwner, target, future, timeoutTaskFuture));
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
        Credentials credentials = credentialsFactory.newCredentials();
        lastCredentials = credentials;
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

    public Credentials getLastCredentials() {
        return lastCredentials;
    }

    Collection<Address> getPossibleMemberAddresses() {
        return clusterConnector.getPossibleMemberAddresses();
    }

    private class TimeoutAuthenticationTask implements Runnable {

        private final ClientInvocationFuture future;

        TimeoutAuthenticationTask(ClientInvocationFuture future) {
            this.future = future;
        }

        @Override
        public void run() {
            if (future.isDone()) {
                return;
            }
            future.complete(new TimeoutException("Authentication response did not come back in "
                    + authenticationTimeout + " millis"));
        }

    }

    private class InitConnectionTask implements Runnable {

        private final Address target;
        private final boolean asOwner;
        private final AuthenticationFuture future;

        InitConnectionTask(Address target, boolean asOwner, AuthenticationFuture future) {
            this.target = target;
            this.asOwner = asOwner;
            this.future = future;
        }

        @Override
        public void run() {
            ClientConnection connection;
            try {
                connection = getConnection(target);
            } catch (Exception e) {
                logger.finest(e);
                future.onFailure(e);
                connectionsInProgress.remove(target);
                return;
            }

            try {
                authenticate(target, connection, asOwner, future);
            } catch (Exception e) {
                future.onFailure(e);
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
        clusterConnector.connectToCluster();
    }

    @Override
    public Future<Void> connectToClusterAsync() {
        return clusterConnector.connectToClusterAsync();
    }


    private class AuthCallback implements ExecutionCallback<ClientMessage> {
        private final ClientConnection connection;
        private final boolean asOwner;
        private final Address target;
        private final AuthenticationFuture future;
        private final ScheduledFuture timeoutTaskFuture;

        AuthCallback(ClientConnection connection, boolean asOwner, Address target,
                     AuthenticationFuture future, ScheduledFuture timeoutTaskFuture) {
            this.connection = connection;
            this.asOwner = asOwner;
            this.target = target;
            this.future = future;
            this.timeoutTaskFuture = timeoutTaskFuture;
        }

        @Override
        public void onResponse(ClientMessage response) {
            timeoutTaskFuture.cancel(true);
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
                        connection.setIsAuthenticatedAsOwner();
                        ClientPrincipal principal = new ClientPrincipal(result.uuid, result.ownerUuid);
                        setPrincipal(principal);
                        //setting owner connection is moved to here(before onAuthenticated/before connected event)
                        //so that invocations that requires owner connection on this connection go through
                        clusterConnector.setOwnerConnectionAddress(connection.getEndPoint());
                        logger.info("Setting " + connection + " as owner with principal " + principal);

                    }
                    onAuthenticated(target, connection);
                    future.onSuccess(connection);
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
            timeoutTaskFuture.cancel(true);
            onAuthenticationFailed(target, connection, t);
            future.onFailure(t);
        }
    }
}

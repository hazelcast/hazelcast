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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.AuthenticationStatus;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.client.config.SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_CLIENT_BUFFER_DIRECT;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final int DEFAULT_SSL_THREAD_COUNT = 3;

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    protected volatile boolean alive;

    private final IOOutOfMemoryHandler outOfMemoryHandler = new IOOutOfMemoryHandler() {
        @Override
        public void handle(OutOfMemoryError error) {
            logger.severe(error);
        }
    };
    private final ILogger logger;
    private final int connectionTimeout;
    private final long heartbeatInterval;
    private final long heartbeatTimeout;

    private final HazelcastClientInstanceImpl client;
    private final SocketInterceptor socketInterceptor;
    private final SocketOptions socketOptions;
    private final SocketChannelWrapperFactory socketChannelWrapperFactory;

    private final ClientExecutionServiceImpl executionService;
    private final AddressTranslator addressTranslator;
    private final ConcurrentMap<Address, ClientConnection> activeConnections
            = new ConcurrentHashMap<Address, ClientConnection>();
    private final ConcurrentMap<Address, AuthenticationFuture> connectionsInProgress =
            new ConcurrentHashMap<Address, AuthenticationFuture>();
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<ConnectionHeartbeatListener> heartbeatListeners =
            new CopyOnWriteArraySet<ConnectionHeartbeatListener>();
    private final Credentials credentials;
    private final AtomicLong correlationIddOfLastAuthentication = new AtomicLong(0);
    private NonBlockingIOThreadingModel ioThreadingModel;

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator) {
        this.client = client;
        this.addressTranslator = addressTranslator;
        this.logger = client.getLoggingService().getLogger(ClientConnectionManager.class);

        ClientConfig config = client.getClientConfig();
        ClientNetworkConfig networkConfig = config.getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        this.connectionTimeout = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        HazelcastProperties hazelcastProperties = client.getProperties();
        long timeout = hazelcastProperties.getMillis(HEARTBEAT_TIMEOUT);
        this.heartbeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());

        long interval = hazelcastProperties.getMillis(HEARTBEAT_INTERVAL);
        this.heartbeatInterval = interval > 0 ? interval : Integer.parseInt(HEARTBEAT_INTERVAL.getDefaultValue());

        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.socketOptions = networkConfig.getSocketOptions();

        initIOThreads(client);

        ClientExtension clientExtension = client.getClientExtension();
        this.socketChannelWrapperFactory = clientExtension.createSocketChannelWrapperFactory();
        this.socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());

        this.credentials = client.getCredentials();
    }

    public NonBlockingIOThreadingModel getIoThreadingModel() {
        return ioThreadingModel;
    }

    protected void initIOThreads(HazelcastClientInstanceImpl client) {
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

        ioThreadingModel = new NonBlockingIOThreadingModel(
                client.getLoggingService(),
                client.getMetricsRegistry(),
                client.getName(),
                outOfMemoryHandler,
                inputThreads,
                outputThreads,
                properties.getInteger(ClientProperty.IO_BALANCER_INTERVAL_SECONDS),
                new ClientSocketWriterInitializer(getBufferSize(), directBuffer),
                new ClientSocketReaderInitializer(getBufferSize(), directBuffer));
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

    @Override
    public synchronized void start() {
        if (alive) {
            return;
        }
        alive = true;
        startIOThreads();
        Heartbeat heartbeat = new Heartbeat();
        executionService.scheduleWithRepetition(heartbeat, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    protected void startIOThreads() {
        ioThreadingModel.start();
    }

    @Override
    public synchronized void shutdown() {
        if (!alive) {
            return;
        }
        alive = false;
        for (ClientConnection connection : activeConnections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }
        shutdownIOThreads();
        connectionListeners.clear();
        heartbeatListeners.clear();
    }

    protected void shutdownIOThreads() {
        ioThreadingModel.shutdown();
    }

    public ClientConnection getConnection(Address target) {
        target = addressTranslator.translate(target);
        if (target == null) {
            return null;
        }
        return activeConnections.get(target);
    }

    @Override
    public Connection getOrConnect(Address address, boolean asOwner) throws IOException {
        try {
            while (true) {
                Connection connection = getConnection(address, asOwner);
                if (connection != null) {
                    return connection;
                }
                AuthenticationFuture firstCallback = triggerConnect(addressTranslator.translate(address), asOwner);
                connection = firstCallback.get(connectionTimeout);
                if (!asOwner) {
                    return connection;
                }
                if (firstCallback.authenticatedAsOwner) {
                    return connection;
                }
            }
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private static class AuthenticationFuture {

        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private Connection connection;
        private Throwable throwable;
        private boolean authenticatedAsOwner;

        void onSuccess(Connection connection, boolean asOwner) {
            this.connection = connection;
            this.authenticatedAsOwner = asOwner;
            countDownLatch.countDown();
        }

        void onFailure(Throwable throwable) {
            this.throwable = throwable;
            countDownLatch.countDown();
        }

        Connection get(int timeout) throws Throwable {
            if (!countDownLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Authentication response did not come back in " + timeout + " millis");
            }
            if (connection != null) {
                return connection;
            }
            assert throwable != null;
            throw throwable;
        }
    }

    @Override
    public Connection getOrTriggerConnect(Address target, boolean asOwner) throws IOException {
        Connection connection = getConnection(target, asOwner);
        if (connection != null) {
            return connection;
        }
        triggerConnect(target, asOwner);
        return null;
    }

    private Connection getConnection(Address target, boolean asOwner) throws IOException {
        if (!asOwner) {
            ensureOwnerConnectionAvailable();
        }

        target = addressTranslator.translate(target);

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

    private void ensureOwnerConnectionAvailable() throws IOException {
        ClientClusterService clusterService = client.getClientClusterService();
        Address ownerAddress = clusterService.getOwnerConnectionAddress();

        boolean isOwnerConnectionAvailable = ownerAddress != null
                && getConnection(ownerAddress) != null;

        if (!isOwnerConnectionAvailable) {
            throw new IOException("Not able to setup owner connection!");
        }
    }

    private AuthenticationFuture triggerConnect(Address target, boolean asOwner) {
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

    private void fireConnectionAddedEvent(ClientConnection connection) {
        for (ConnectionListener connectionListener : connectionListeners) {
            connectionListener.connectionAdded(connection);
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
            socketChannel.socket().connect(inetSocketAddress, connectionTimeout);
            SocketChannelWrapper socketChannelWrapper =
                    socketChannelWrapperFactory.wrapSocketChannel(socketChannel, true);

            final ClientConnection clientConnection = new ClientConnection(
                    client, ioThreadingModel, connectionIdGen.incrementAndGet(), socketChannelWrapper);
            socketChannel.configureBlocking(true);
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }
            socketChannel.configureBlocking(ioThreadingModel.isBlocking());
            socket.setSoTimeout(0);
            clientConnection.start();
            return clientConnection;
        } catch (Exception e) {
            if (socketChannel != null) {
                socketChannel.close();
            }
            throw ExceptionUtil.rethrow(e, IOException.class);
        }
    }

    private int getBufferSize() {
        int bufferSize = socketOptions.getBufferSize() * KILO_BYTE;
        if (bufferSize <= 0) {
            bufferSize = DEFAULT_BUFFER_SIZE_BYTE;
        }
        return bufferSize;
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

            for (ConnectionListener listener : connectionListeners) {
                listener.connectionRemoved(connection);
            }
        } else {
            if (logger.isFinestEnabled()) {
                logger.finest("Destroying a connection, but there is no mapping " + endpoint + " -> " + connection
                        + " in the connection map.");
            }
        }
    }

    @Override
    public void handleClientMessage(ClientMessage message, Connection connection) {
        ClientConnection conn = (ClientConnection) connection;
        ClientInvocationService invocationService = client.getInvocationService();
        conn.incrementPendingPacketCount();
        if (message.isFlagSet(ClientMessage.LISTENER_EVENT_FLAG)) {
            ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
            listenerService.handleClientMessage(message, connection);
        } else {
            invocationService.handleClientMessage(message, connection);
        }
    }

    class Heartbeat implements Runnable {

        @Override
        public void run() {
            if (!alive) {
                return;
            }
            final long now = Clock.currentTimeMillis();
            for (final ClientConnection connection : activeConnections.values()) {
                if (!connection.isAlive()) {
                    continue;
                }

                if (now - connection.lastReadTimeMillis() > heartbeatTimeout) {
                    if (connection.isHeartBeating()) {
                        logger.warning("Heartbeat failed to connection: " + connection);
                        connection.onHeartbeatFailed();
                        fireHeartbeatStopped(connection);
                    }
                }
                if (now - connection.lastReadTimeMillis() > heartbeatInterval) {
                    ClientMessage request = ClientPingCodec.encodeRequest();
                    final ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
                    clientInvocation.setBypassHeartbeatCheck(true);
                    connection.onHeartbeatRequested();
                    clientInvocation.invokeUrgent().andThen(new ExecutionCallback<ClientMessage>() {
                        @Override
                        public void onResponse(ClientMessage response) {
                            if (connection.isAlive()) {
                                connection.onHeartbeatReceived();
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            if (connection.isAlive()) {
                                logger.warning("Error receiving heartbeat for connection: " + connection, t);
                            }
                        }
                    });
                } else {
                    if (!connection.isHeartBeating()) {
                        logger.warning("Heartbeat is back to healthy for connection: " + connection);
                        connection.onHeartbeatResumed();
                        fireHeartbeatResumed(connection);
                    }
                }
            }
        }

        private void fireHeartbeatResumed(ClientConnection connection) {
            for (ConnectionHeartbeatListener heartbeatListener : heartbeatListeners) {
                heartbeatListener.heartbeatResumed(connection);
            }
        }

        private void fireHeartbeatStopped(ClientConnection connection) {
            for (ConnectionHeartbeatListener heartbeatListener : heartbeatListeners) {
                heartbeatListener.heartbeatStopped(connection);
            }
        }

    }

    @Override
    public void addConnectionListener(ConnectionListener connectionListener) {
        connectionListeners.add(connectionListener);
    }

    @Override
    public void addConnectionHeartbeatListener(ConnectionHeartbeatListener connectionHeartbeatListener) {
        heartbeatListeners.add(connectionHeartbeatListener);
    }

    private void authenticate(final Address target, final ClientConnection connection, final boolean asOwner,
                              final AuthenticationFuture callback) {
        SerializationService ss = client.getSerializationService();
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        final ClientPrincipal principal = clusterService.getPrincipal();
        byte serializationVersion = ((InternalSerializationService) client.getSerializationService()).getVersion();
        String uuid = null;
        String ownerUuid = null;
        if (principal != null) {
            uuid = principal.getUuid();
            ownerUuid = principal.getOwnerUuid();
        }
        ClientMessage clientMessage = encodeAuthenticationRequest(asOwner, ss, serializationVersion, uuid, ownerUuid);
        ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, connection);
        ClientInvocationFuture future = clientInvocation.invokeUrgent();
        if (asOwner && clientInvocation.getSendConnection() != null) {
            correlationIddOfLastAuthentication.set(clientInvocation.getClientMessage().getCorrelationId());
        }
        future.andThen(new ExecutionCallback<ClientMessage>() {
            @Override
            public void onResponse(ClientMessage response) {
                ClientAuthenticationCodec.ResponseParameters result = ClientAuthenticationCodec.decodeResponse(response);
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
                            clusterService.setPrincipal(principal);
                            clusterService.setOwnerConnectionAddress(connection.getEndPoint());
                            logger.info("Setting " + connection + " as owner  with principal " + principal);
                        }
                        onAuthenticated(target, connection);
                        callback.onSuccess(connection, asOwner);
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

    private ClientMessage encodeAuthenticationRequest(boolean asOwner, SerializationService ss, byte serializationVersion,
                                                      String uuid, String ownerUuid) {
        ClientMessage clientMessage;
        if (credentials.getClass().equals(UsernamePasswordCredentials.class)) {
            UsernamePasswordCredentials cr = (UsernamePasswordCredentials) credentials;
            clientMessage = ClientAuthenticationCodec
                    .encodeRequest(cr.getUsername(), cr.getPassword(), uuid, ownerUuid, asOwner, ClientTypes.JAVA,
                            serializationVersion, BuildInfoProvider.BUILD_INFO.getVersion());
        } else {
            Data data = ss.toData(credentials);
            clientMessage = ClientAuthenticationCustomCodec.encodeRequest(data, uuid, ownerUuid,
                    asOwner, ClientTypes.JAVA, serializationVersion, BuildInfoProvider.BUILD_INFO.getVersion());
        }
        return clientMessage;
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
            ClientConnection connection = activeConnections.get(target);
            if (connection == null) {
                try {
                    connection = createSocketConnection(target);
                } catch (Exception e) {
                    logger.finest(e);
                    callback.onFailure(e);
                    connectionsInProgress.remove(target);
                    return;
                }
            }

            try {
                authenticate(target, connection, asOwner, callback);
            } catch (Exception e) {
                callback.onFailure(e);
                connection.close("Failed to authenticate connection", e);
                connectionsInProgress.remove(target);
            }
        }
    }

    private void onAuthenticated(Address target, ClientConnection connection) {
        ClientConnection oldConnection =
                activeConnections.put(addressTranslator.translate(connection.getEndPoint()), connection);
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


}

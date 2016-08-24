/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadOutOfMemoryHandler;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT;

/**
 * Implementation of {@link ClientConnectionManager}.
 */
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    protected final AtomicInteger connectionIdGen = new AtomicInteger();

    protected volatile boolean alive;

    private final NonBlockingIOThreadOutOfMemoryHandler outOfMemoryHandler = new NonBlockingIOThreadOutOfMemoryHandler() {
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
    private NonBlockingIOThread inputThread;
    private NonBlockingIOThread outputThread;
    private final SocketChannelWrapperFactory socketChannelWrapperFactory;

    private final ClientExecutionServiceImpl executionService;
    private final AddressTranslator addressTranslator;
    private final ConcurrentMap<Address, ClientConnection> connections
            = new ConcurrentHashMap<Address, ClientConnection>();
    private final ConcurrentMap<Address, AuthenticationFuture> connectionsInProgress =
            new ConcurrentHashMap<Address, AuthenticationFuture>();
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<ConnectionHeartbeatListener> heartbeatListeners =
            new CopyOnWriteArraySet<ConnectionHeartbeatListener>();
    private final LoggingService loggingService;
    private final Credentials credentials;

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator) {
        this.client = client;
        this.addressTranslator = addressTranslator;
        final ClientConfig config = client.getClientConfig();
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        connectionTimeout = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        HazelcastProperties hazelcastProperties = client.getProperties();
        long timeout = hazelcastProperties.getMillis(HEARTBEAT_TIMEOUT);
        this.heartbeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());

        long interval = hazelcastProperties.getMillis(HEARTBEAT_INTERVAL);
        heartbeatInterval = interval > 0 ? interval : Integer.parseInt(HEARTBEAT_INTERVAL.getDefaultValue());

        executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        loggingService = client.getLoggingService();

        initializeSelectors(client);

        socketOptions = networkConfig.getSocketOptions();
        ClientExtension clientExtension = client.getClientExtension();
        socketChannelWrapperFactory = clientExtension.createSocketChannelWrapperFactory();
        socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
        logger = loggingService.getLogger(ClientConnectionManager.class);
        credentials = client.getCredentials();
    }

    protected void initializeSelectors(HazelcastClientInstanceImpl client) {
        inputThread = new NonBlockingIOThread(
                client.getThreadGroup(),
                client.getName() + ".thread-in",
                loggingService.getLogger(NonBlockingIOThread.class),
                outOfMemoryHandler);
        client.getMetricsRegistry().scanAndRegister(inputThread, "tcp." + inputThread.getName());

        outputThread = new ClientNonBlockingOutputThread(
                client.getThreadGroup(),
                client.getName() + ".thread-out",
                loggingService.getLogger(ClientNonBlockingOutputThread.class),
                outOfMemoryHandler);
        client.getMetricsRegistry().scanAndRegister(outputThread, "tcp." + outputThread.getName());
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            ClientExtension clientExtension = client.getClientExtension();
            return clientExtension.createSocketInterceptor();
        }
        return null;
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
        startSelectors();
        Heartbeat heartbeat = new Heartbeat();
        executionService.scheduleWithRepetition(heartbeat, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    protected void startSelectors() {
        inputThread.start();
        outputThread.start();
    }

    @Override
    public synchronized void shutdown() {
        if (!alive) {
            return;
        }
        alive = false;
        for (ClientConnection connection : connections.values()) {
            connection.close("Hazelcast client is shutting down", null);
        }
        shutdownSelectors();
        connectionListeners.clear();
        heartbeatListeners.clear();
    }

    protected void shutdownSelectors() {
        inputThread.shutdown();
        outputThread.shutdown();
    }

    public ClientConnection getConnection(Address target) {
        target = addressTranslator.translate(target);
        if (target == null) {
            return null;
        }
        return connections.get(target);
    }

    @Override
    public Connection getOrConnect(Address address, boolean asOwner) throws IOException {
        try {
            while (true) {
                Connection connection = getConnection(address, asOwner);
                if (connection != null) {
                    return connection;
                }
                AuthenticationFuture firstCallback = triggerConnect(address, asOwner);
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

        public void onSuccess(Connection connection, boolean asOwner) {
            this.connection = connection;
            this.authenticatedAsOwner = asOwner;
            countDownLatch.countDown();
        }

        public void onFailure(Throwable throwable) {
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
            throw throwable;
        }
    }

    @Override
    public Connection getOrTriggerConnect(Address target, boolean asOwner) {
        Connection connection = getConnection(target, asOwner);
        if (connection != null) {
            return connection;
        }
        triggerConnect(target, asOwner);
        return null;
    }

    private Connection getConnection(Address target, boolean asOwner) {
        target = addressTranslator.translate(target);

        if (target == null) {
            throw new IllegalStateException("Address can not be null");
        }

        ClientConnection connection = connections.get(target);

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

    private AuthenticationFuture triggerConnect(Address target, boolean asOwner) {
        if (!alive) {
            throw new HazelcastException("ConnectionManager is not active!!!");
        }

        AuthenticationFuture callback = new AuthenticationFuture();
        AuthenticationFuture firstCallback = connectionsInProgress.putIfAbsent(target, callback);
        if (firstCallback == null) {
            ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
            executionService.executeInternal(new InitConnectionTask(target, asOwner, callback));
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
            throw new HazelcastException("ConnectionManager is not active!!!");
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
            int bufferSize = socketOptions.getBufferSize() * KILO_BYTE;
            if (bufferSize <= 0) {
                bufferSize = DEFAULT_BUFFER_SIZE_BYTE;
            }
            socket.setSendBufferSize(bufferSize);
            socket.setReceiveBufferSize(bufferSize);
            InetSocketAddress inetSocketAddress = address.getInetSocketAddress();
            socketChannel.socket().connect(inetSocketAddress, connectionTimeout);
            SocketChannelWrapper socketChannelWrapper =
                    socketChannelWrapperFactory.wrapSocketChannel(socketChannel, true);
            final ClientConnection clientConnection = new ClientConnection(client, inputThread,
                    outputThread, connectionIdGen.incrementAndGet(), socketChannelWrapper);
            socketChannel.configureBlocking(true);
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }
            socketChannel.configureBlocking(false);
            socket.setSoTimeout(0);
            clientConnection.getReadHandler().register();
            clientConnection.init();
            return clientConnection;
        } catch (Exception e) {
            if (socketChannel != null) {
                socketChannel.close();
            }
            throw ExceptionUtil.rethrow(e, IOException.class);
        }
    }

    @Override
    public void destroyConnection(final Connection connection, final String reason, final Throwable cause) {
        Address endPoint = connection.getEndPoint();
        ClientConnection conn = (ClientConnection) connection;
        if (endPoint != null && connections.remove(endPoint, conn)) {
            logger.info("Removed connection to endpoint: " + endPoint + ", connection: " + connection);

            conn.close(reason, cause);
            for (ConnectionListener connectionListener : connectionListeners) {
                connectionListener.connectionRemoved(conn);
            }
        } else {
            connection.close(reason, cause);
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
            for (final ClientConnection connection : connections.values()) {
                if (now - connection.lastReadTimeMillis() > heartbeatTimeout) {
                    if (connection.isHeartBeating()) {
                        logger.warning("Heartbeat failed to connection : " + connection);
                        connection.onHeartbeatFailed();
                        fireHeartbeatStopped(connection);
                    }
                }
                if (now - connection.lastReadTimeMillis() > heartbeatInterval) {
                    ClientMessage request = ClientPingCodec.encodeRequest();
                    ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
                    clientInvocation.setBypassHeartbeatCheck(true);
                    connection.onHeartbeatRequested();
                    clientInvocation.invokeUrgent().andThen(new ExecutionCallback<ClientMessage>() {
                        @Override
                        public void onResponse(ClientMessage response) {
                            connection.onHeartbeatReceived();
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            logger.warning("Error receiving heartbeat for connection: " + connection, t);
                        }
                    });
                } else {
                    if (!connection.isHeartBeating()) {
                        logger.warning("Heartbeat is back to healthy for connection : " + connection);
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

    private void authenticate(final Address target, final ClientConnection connection,
                              final boolean asOwner,
                              final AuthenticationFuture callback) {
        SerializationService ss = client.getSerializationService();
        final ClientClusterServiceImpl clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        ClientPrincipal principal = clusterService.getPrincipal();
        byte serializationVersion = ((InternalSerializationService) client.getSerializationService()).getVersion();

        String uuid = null;
        String ownerUuid = null;
        if (principal != null) {
            uuid = principal.getUuid();
            ownerUuid = principal.getOwnerUuid();
        }

        ClientMessage clientMessage;
        if (credentials.getClass().equals(UsernamePasswordCredentials.class)) {
            UsernamePasswordCredentials cr = (UsernamePasswordCredentials) credentials;
            clientMessage = ClientAuthenticationCodec.encodeRequest(cr.getUsername(), cr.getPassword(),
                    uuid, ownerUuid, asOwner, ClientTypes.JAVA, serializationVersion);
        } else {
            Data data = ss.toData(credentials);
            clientMessage = ClientAuthenticationCustomCodec.encodeRequest(data, uuid, ownerUuid,
                    asOwner, ClientTypes.JAVA, serializationVersion);

        }
        ClientInvocation clientInvocation = new ClientInvocation(client, clientMessage, connection);
        ClientInvocationFuture future = clientInvocation.invokeUrgent();
        future.andThen(new ExecutionCallback<ClientMessage>() {
            @Override
            public void onResponse(ClientMessage response) {
                ClientAuthenticationCodec.ResponseParameters result = ClientAuthenticationCodec.decodeResponse(response);
                AuthenticationStatus authenticationStatus = AuthenticationStatus.getById(result.status);
                switch (authenticationStatus) {
                    case AUTHENTICATED:
                        connection.setRemoteEndpoint(result.address);
                        if (asOwner) {
                            connection.setIsAuthenticatedAsOwner();
                            clusterService.setPrincipal(new ClientPrincipal(result.uuid, result.ownerUuid));
                        }
                        authenticated(target, connection);
                        callback.onSuccess(connection, asOwner);
                        break;
                    case CREDENTIALS_FAILED:
                        AuthenticationException e = new AuthenticationException("Invalid credentials!");
                        failed(target, connection, e);
                        callback.onFailure(e);
                        break;
                    default:
                        AuthenticationException exception =
                                new AuthenticationException("Authentication status code not supported. status:"
                                        + authenticationStatus);
                        failed(target, connection, exception);
                        callback.onFailure(exception);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                failed(target, connection, t);
                callback.onFailure(t);
            }
        }, executionService.getInternalExecutor());
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
            ClientConnection connection = connections.get(target);
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
                destroyConnection(connection, "Failed to authenticate connection", e);
                connectionsInProgress.remove(target);
            }
        }
    }

    private void authenticated(Address target, ClientConnection connection) {
        ClientConnection oldConnection = connections.put(connection.getRemoteEndpoint(), connection);
        if (oldConnection == null) {
            fireConnectionAddedEvent(connection);
        }
        assert oldConnection == null || connection.equals(oldConnection);
        connectionsInProgress.remove(target);
    }

    private void failed(Address target, ClientConnection connection, Throwable cause) {
        logger.finest(cause);
        destroyConnection(connection, null, cause);
        connectionsInProgress.remove(target);
    }


}

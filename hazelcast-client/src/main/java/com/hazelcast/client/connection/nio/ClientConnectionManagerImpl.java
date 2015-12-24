/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadOutOfMemoryHandler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientProperty.HEARTBEAT_INTERVAL;
import static com.hazelcast.client.config.ClientProperty.HEARTBEAT_TIMEOUT;
import static com.hazelcast.client.config.SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;

public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final ILogger LOGGER = Logger.getLogger(ClientConnectionManagerImpl.class);

    private static final NonBlockingIOThreadOutOfMemoryHandler OUT_OF_MEMORY_HANDLER = new NonBlockingIOThreadOutOfMemoryHandler() {
        @Override
        public void handle(OutOfMemoryError error) {
            LOGGER.severe(error);
        }
    };

    private final int connectionTimeout;
    private final long heartBeatInterval;
    private final long heartBeatTimeout;

    private final ConcurrentMap<Address, Object> connectionLockMap = new ConcurrentHashMap<Address, Object>();

    protected final AtomicInteger connectionIdGen = new AtomicInteger();
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
    private final Set<Address> connectionsInProgress =
            Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();
    private final Set<ConnectionHeartbeatListener> heartbeatListeners =
            new CopyOnWriteArraySet<ConnectionHeartbeatListener>();

    protected volatile boolean alive;

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client, AddressTranslator addressTranslator) {
        this.client = client;
        this.addressTranslator = addressTranslator;
        final ClientConfig config = client.getClientConfig();
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        connectionTimeout = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        ClientProperties clientProperties = client.getClientProperties();
        long timeout = clientProperties.getMillis(HEARTBEAT_TIMEOUT);
        this.heartBeatTimeout = timeout > 0 ? timeout : Integer.parseInt(HEARTBEAT_TIMEOUT.getDefaultValue());

        long interval = clientProperties.getMillis(HEARTBEAT_INTERVAL);
        heartBeatInterval = interval > 0 ? interval : Integer.parseInt(HEARTBEAT_INTERVAL.getDefaultValue());

        executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();

        initializeSelectors(client);

        socketOptions = networkConfig.getSocketOptions();
        ClientExtension clientExtension = client.getClientExtension();
        socketChannelWrapperFactory = clientExtension.createSocketChannelWrapperFactory();
        socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
    }

    protected void initializeSelectors(HazelcastClientInstanceImpl client) {
        inputThread = new NonBlockingIOThread(
                client.getThreadGroup(),
                client.getName() + ".ClientInSelector",
                Logger.getLogger(NonBlockingIOThread.class),
                OUT_OF_MEMORY_HANDLER);
        outputThread = new ClientNonBlockingOutputThread(
                client.getThreadGroup(),
                client.getName() + ".ClientOutSelector",
                Logger.getLogger(ClientNonBlockingOutputThread.class),
                OUT_OF_MEMORY_HANDLER);
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
        HeartBeat heartBeat = new HeartBeat();
        executionService.scheduleWithFixedDelay(heartBeat, heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
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
            connection.close();
        }
        shutdownSelectors();
        connectionLockMap.clear();
        connectionListeners.clear();
        heartbeatListeners.clear();
    }

    protected void shutdownSelectors() {
        inputThread.shutdown();
        outputThread.shutdown();
    }

    public ClientConnection getConnection(Address target) {
        return connections.get(target);
    }

    public ClientConnection getOrConnect(Address target, Authenticator authenticator) throws IOException {
        Address remoteAddress = addressTranslator.translate(target);

        if (remoteAddress == null) {
            throw new IOException("Address is required!");
        }

        ClientConnection connection = connections.get(target);
        Object lock = getLock(target);

        if (connection == null) {
            synchronized (lock) {
                connection = connections.get(target);
                if (connection == null) {
                    connection = initializeConnection(remoteAddress, authenticator);
                }
            }
        }
        return connection;
    }

    private ClientConnection initializeConnection(Address address, Authenticator authenticator) throws IOException {
        ClientConnection connection = createSocketConnection(address);
        authenticate(authenticator, connection);
        connections.put(connection.getRemoteEndpoint(), connection);
        fireConnectionAddedEvent(connection);
        return connection;
    }

    public ClientConnection getOrTriggerConnect(Address target, Authenticator authenticator) throws IOException {
        Address remoteAddress = addressTranslator.translate(target);

        if (remoteAddress == null) {
            throw new IOException("Address is required!");
        }

        ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        ClientConnection connection = connections.get(target);

        if (connection != null) {
            return connection;
        }

        if (connectionsInProgress.add(target)) {
            executionService.executeInternal(new InitConnectionTask(target, remoteAddress, authenticator));
        }

        throw new IOException("No available connection to address " + target);
    }

    private void authenticate(Authenticator authenticator, ClientConnection connection) throws IOException {
        try {
            authenticator.authenticate(connection);
        } catch (Throwable throwable) {
            connection.close(throwable);
            throw ExceptionUtil.rethrow(throwable, IOException.class);
        }
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
    public void destroyConnection(final Connection connection) {
        Address endpoint = connection.getEndPoint();
        if (endpoint != null) {
            final ClientConnection conn = connections.remove(endpoint);
            if (conn == null) {
                return;
            }
            conn.close();
            for (ConnectionListener connectionListener : connectionListeners) {
                connectionListener.connectionRemoved(conn);
            }

        } else {
            ClientInvocationService invocationService = client.getInvocationService();
            invocationService.cleanConnectionResources((ClientConnection) connection);
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

    private Object getLock(Address address) {
        Object lock = connectionLockMap.get(address);
        if (lock == null) {
            lock = new Object();
            Object current = connectionLockMap.putIfAbsent(address, lock);
            if (current != null) {
                lock = current;
            }
        }
        return lock;
    }

    class HeartBeat implements Runnable {

        @Override
        public void run() {
            if (!alive) {
                return;
            }
            final long now = Clock.currentTimeMillis();
            for (ClientConnection connection : connections.values()) {
                if (now - connection.lastReadTimeMillis() > heartBeatTimeout) {
                    if (connection.isHeartBeating()) {
                        LOGGER.warning("Heartbeat failed to connection : " + connection);
                        connection.heartBeatingFailed();
                        fireHeartBeatStopped(connection);
                    }
                }
                if (now - connection.lastReadTimeMillis() > heartBeatInterval) {
                    ClientMessage request = ClientPingCodec.encodeRequest();
                    ClientInvocation clientInvocation = new ClientInvocation(client, request, connection);
                    clientInvocation.setBypassHeartbeatCheck(true);
                    clientInvocation.invokeUrgent();
                } else {
                    if (!connection.isHeartBeating()) {
                        LOGGER.warning("Heartbeat is back to healthy for connection : " + connection);
                        connection.heartBeatingSucceed();
                        fireHeartBeatStarted(connection);
                    }
                }
            }
        }

        private void fireHeartBeatStarted(ClientConnection connection) {
            for (ConnectionHeartbeatListener heartbeatListener : heartbeatListeners) {
                heartbeatListener.heartBeatStarted(connection);
            }
        }

        private void fireHeartBeatStopped(ClientConnection connection) {
            for (ConnectionHeartbeatListener heartbeatListener : heartbeatListeners) {
                heartbeatListener.heartBeatStopped(connection);
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

    private class InitConnectionTask implements Runnable {

        private final Address target;
        private final Address remoteAddress;
        private final Authenticator authenticator;

        InitConnectionTask(Address target, Address remoteAddress, Authenticator authenticator) {
            this.target = target;
            this.remoteAddress = remoteAddress;
            this.authenticator = authenticator;
        }

        @Override
        public void run() {
            final Object lock = getLock(target);
            synchronized (lock) {
                ClientConnection connection = connections.get(target);
                if (connection != null) {
                    return;
                }
                try {
                    initializeConnection(remoteAddress, authenticator);
                } catch (IOException e) {
                    LOGGER.finest(e);
                } finally {
                    connectionsInProgress.remove(target);
                }

            }
        }
    }
}

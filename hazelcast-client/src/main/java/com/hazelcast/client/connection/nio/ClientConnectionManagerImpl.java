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
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientListenerServiceImpl;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.IOSelectorOutOfMemoryHandler;
import com.hazelcast.nio.tcp.InSelectorImpl;
import com.hazelcast.nio.tcp.OutSelectorImpl;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_INTERVAL_DEFAULT;
import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_TIMEOUT_DEFAULT;
import static com.hazelcast.client.config.SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;

public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final ILogger LOGGER = Logger.getLogger(ClientConnectionManagerImpl.class);

    private static final IOSelectorOutOfMemoryHandler OUT_OF_MEMORY_HANDLER = new IOSelectorOutOfMemoryHandler() {
        @Override
        public void handle(OutOfMemoryError error) {
            LOGGER.severe(error);
        }
    };

    private final int connectionTimeout;
    private final int heartBeatInterval;
    private final int heartBeatTimeout;

    private final ConcurrentMap<Address, Object> connectionLockMap = new ConcurrentHashMap<Address, Object>();

    private final AtomicInteger connectionIdGen = new AtomicInteger();
    private final HazelcastClientInstanceImpl client;
    private final SocketInterceptor socketInterceptor;
    private final SocketOptions socketOptions;
    private final IOSelector inSelector;
    private final IOSelector outSelector;

    private final SocketChannelWrapperFactory socketChannelWrapperFactory;
    private final ClientExecutionServiceImpl executionService;
    private final AddressTranslator addressTranslator;
    private final ConcurrentMap<Address, ClientConnection> connections
            = new ConcurrentHashMap<Address, ClientConnection>();

    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();
    private final Set<ConnectionHeartbeatListener> heartbeatListeners =
            new CopyOnWriteArraySet<ConnectionHeartbeatListener>();

    private volatile boolean alive;

    public ClientConnectionManagerImpl(HazelcastClientInstanceImpl client,
                                       AddressTranslator addressTranslator) {
        this.client = client;
        this.addressTranslator = addressTranslator;
        final ClientConfig config = client.getClientConfig();
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        connectionTimeout = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        final ClientProperties clientProperties = client.getClientProperties();
        int timeout = clientProperties.getHeartbeatTimeout().getInteger();
        this.heartBeatTimeout = timeout > 0 ? timeout : Integer.parseInt(PROP_HEARTBEAT_TIMEOUT_DEFAULT);

        int interval = clientProperties.getHeartbeatInterval().getInteger();
        heartBeatInterval = interval > 0 ? interval : Integer.parseInt(PROP_HEARTBEAT_INTERVAL_DEFAULT);

        executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();

        inSelector = new InSelectorImpl(
                client.getThreadGroup(),
                "InSelector",
                Logger.getLogger(InSelectorImpl.class),
                OUT_OF_MEMORY_HANDLER);
        outSelector = new OutSelectorImpl(
                client.getThreadGroup(),
                "OutSelector",
                Logger.getLogger(OutSelectorImpl.class),
                OUT_OF_MEMORY_HANDLER);

        socketOptions = networkConfig.getSocketOptions();
        ClientExtension clientExtension = client.getClientExtension();
        socketChannelWrapperFactory = clientExtension.getSocketChannelWrapperFactory();
        socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
    }


    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            ClientExtension clientExtension = client.getClientExtension();
            return clientExtension.getSocketInterceptor();
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
        inSelector.start();
        outSelector.start();
        HeartBeat heartBeat = new HeartBeat();
        executionService.scheduleWithFixedDelay(heartBeat, heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
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
        inSelector.shutdown();
        outSelector.shutdown();
        connectionLockMap.clear();
        connectionListeners.clear();
        heartbeatListeners.clear();
    }

    public ClientConnection getConnection(Address target) {
        return connections.get(target);
    }

    public ClientConnection getOrConnect(Address target, Authenticator authenticator) throws IOException {
        Address address = addressTranslator.translate(target);

        if (address == null) {
            throw new IOException("Address is required!");
        }

        ClientConnection connection = connections.get(target);
        if (connection == null) {
            final Object lock = getLock(target);
            synchronized (lock) {
                connection = connections.get(target);
                if (connection == null) {
                    connection = createSocketConnection(address);
                    authenticate(authenticator, connection);
                    connections.put(connection.getRemoteEndpoint(), connection);
                    fireConnectionAddedEvent(connection);
                }
            }
        }
        return connection;
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

    private ClientConnection createSocketConnection(final Address address) throws IOException {
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
            socketChannel.socket().connect(address.getInetSocketAddress(), connectionTimeout);
            SocketChannelWrapper socketChannelWrapper =
                    socketChannelWrapperFactory.wrapSocketChannel(socketChannel, true);
            final ClientConnection clientConnection = new ClientConnection(client, inSelector,
                    outSelector, connectionIdGen.incrementAndGet(), socketChannelWrapper);
            socketChannel.configureBlocking(true);
            if (socketInterceptor != null) {
                socketInterceptor.onConnect(socket);
            }
            socketChannel.configureBlocking(false);
            socket.setSoTimeout(0);
            clientConnection.getReadHandler().register();
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
    public void handlePacket(Packet packet) {
        final ClientConnection conn = (ClientConnection) packet.getConn();
        conn.incrementPacketCount();
        if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
            final ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
            listenerService.handleEventPacket(packet);
        } else {
            ClientInvocationService invocationService = client.getInvocationService();
            invocationService.handlePacket(packet);
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
                if (now - connection.lastReadTime() > heartBeatTimeout) {
                    if (connection.isHeartBeating()) {
                        connection.heartBeatingFailed();
                        fireHeartBeatStopped(connection);
                    }
                }
                if (now - connection.lastReadTime() > heartBeatInterval) {
                    final ClientPingRequest request = new ClientPingRequest();
                    new ClientInvocation(client, request, connection).invoke();
                } else {
                    if (!connection.isHeartBeating()) {
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
}

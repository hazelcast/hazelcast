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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.cluster.impl.BindMessage;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.counters.MwCounter;
import com.hazelcast.util.executor.StripedRunnable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.counters.MwCounter.newMwCounter;

public class TcpIpConnectionManager implements ConnectionManager, PacketHandler {

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    final LoggingService loggingService;

    @Probe(name = "connectionListenerCount")
    final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final IOService ioService;

    private final ConstructorFunction<Address, TcpIpConnectionMonitor> monitorConstructor
            = new ConstructorFunction<Address, TcpIpConnectionMonitor>() {
        public TcpIpConnectionMonitor createNew(Address endpoint) {
            return new TcpIpConnectionMonitor(TcpIpConnectionManager.this, endpoint);
        }
    };

    private final ILogger logger;

    @Probe(name = "count", level = MANDATORY)
    private final ConcurrentHashMap<Address, Connection> connectionsMap = new ConcurrentHashMap<Address, Connection>(100);

    @Probe(name = "monitorCount")
    private final ConcurrentHashMap<Address, TcpIpConnectionMonitor> monitors =
            new ConcurrentHashMap<Address, TcpIpConnectionMonitor>(100);

    @Probe(name = "inProgressCount")
    private final Set<Address> connectionsInProgress =
            Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    @Probe(name = "acceptedSocketCount", level = MANDATORY)
    private final Set<SocketChannelWrapper> acceptedSockets =
            Collections.newSetFromMap(new ConcurrentHashMap<SocketChannelWrapper, Boolean>());

    @Probe(name = "activeCount", level = MANDATORY)
    private final Set<TcpIpConnection> activeConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<TcpIpConnection, Boolean>());

    @Probe(name = "textCount", level = MANDATORY)
    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private final IOThreadingModel ioThreadingModel;

    private volatile boolean live;

    private final ServerSocketChannel serverSocketChannel;

    private final SocketChannelWrapperFactory socketChannelWrapperFactory;

    private final int outboundPortCount;

    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();

    // accessed only in synchronized block
    private volatile SocketAcceptorThread acceptorThread;

    @Probe
    private final MwCounter openedCount = newMwCounter();
    @Probe
    private final MwCounter closedCount = newMwCounter();

    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(4);

    public TcpIpConnectionManager(IOService ioService,
                                  ServerSocketChannel serverSocketChannel,
                                  MetricsRegistry metricsRegistry,
                                  HazelcastThreadGroup threadGroup,
                                  LoggingService loggingService) {
        this(ioService, serverSocketChannel, loggingService, metricsRegistry,
                new NonBlockingIOThreadingModel(ioService, loggingService, metricsRegistry, threadGroup));
    }

    public TcpIpConnectionManager(IOService ioService,
                                  ServerSocketChannel serverSocketChannel,
                                  LoggingService loggingService,
                                  MetricsRegistry metricsRegistry,
                                  IOThreadingModel ioThreadingModel) {
        this.ioService = ioService;
        this.ioThreadingModel = ioThreadingModel;
        this.serverSocketChannel = serverSocketChannel;
        this.loggingService = loggingService;
        this.logger = loggingService.getLogger(TcpIpConnectionManager.class);
        final Collection<Integer> ports = ioService.getOutboundPorts();
        this.outboundPortCount = ports.size();
        this.outboundPorts.addAll(ports);
        this.socketChannelWrapperFactory = ioService.getSocketChannelWrapperFactory();

        metricsRegistry.scanAndRegister(this, "tcp.connection");
    }

    public IOService getIoService() {
        return ioService;
    }

    public IOThreadingModel getIoThreadingModel() {
        return ioThreadingModel;
    }

    public void interceptSocket(Socket socket, boolean onAccept) throws IOException {
        if (!isSocketInterceptorEnabled()) {
            return;
        }
        final MemberSocketInterceptor memberSocketInterceptor = ioService.getMemberSocketInterceptor();
        if (memberSocketInterceptor == null) {
            return;
        }
        if (onAccept) {
            memberSocketInterceptor.onAccept(socket);
        } else {
            memberSocketInterceptor.onConnect(socket);
        }
    }

    public boolean isSocketInterceptorEnabled() {
        final SocketInterceptorConfig socketInterceptorConfig = ioService.getSocketInterceptorConfig();
        if (socketInterceptorConfig != null && socketInterceptorConfig.isEnabled()) {
            return true;
        }
        return false;
    }

    // just for testing
    public Set<TcpIpConnection> getActiveConnections() {
        return activeConnections;
    }

    @Override
    public int getActiveConnectionCount() {
        return activeConnections.size();
    }

    public int getAllTextConnections() {
        return allTextConnections.get();
    }

    @Override
    public int getConnectionCount() {
        return connectionsMap.size();
    }

    public boolean isSSLEnabled() {
        return socketChannelWrapperFactory.isSSlEnabled();
    }

    public void incrementTextConnections() {
        allTextConnections.incrementAndGet();
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        checkNotNull(listener, "listener can't be null");
        connectionListeners.add(listener);
    }

    @Override
    public void handle(Packet packet) throws Exception {
        assert packet.isHeaderSet(Packet.HEADER_BIND);

        BindMessage bind = ioService.getSerializationService().toObject(packet);
        bind((TcpIpConnection) packet.getConn(), bind.getLocalAddress(), bind.getTargetAddress(), bind.shouldReply());
    }

    /**
     * Binding completes the connection and makes it available to be used with the ConnectionManager.
     */
    private boolean bind(TcpIpConnection connection, Address remoteEndPoint, Address localEndpoint, boolean reply) {
        if (logger.isFinestEnabled()) {
            logger.finest("Binding " + connection + " to " + remoteEndPoint + ", reply is " + reply);
        }
        final Address thisAddress = ioService.getThisAddress();
        if (ioService.isSocketBindAny() && !connection.isClient() && !thisAddress.equals(localEndpoint)) {
            logger.warning("Wrong bind request from " + remoteEndPoint
                    + "! This node is not requested endpoint: " + localEndpoint);
            connection.close();
            return false;
        }
        connection.setEndPoint(remoteEndPoint);
        ioService.onSuccessfulConnection(remoteEndPoint);
        if (reply) {
            sendBindRequest(connection, remoteEndPoint, false);
        }
        if (checkAlreadyConnected(connection, remoteEndPoint)) {
            return false;
        }
        return registerConnection(remoteEndPoint, connection);
    }

    @Override
    public boolean registerConnection(final Address remoteEndPoint, final Connection connection) {
        if (remoteEndPoint.equals(ioService.getThisAddress())) {
            return false;
        }

        if (connection instanceof TcpIpConnection) {
            TcpIpConnection tcpConnection = (TcpIpConnection) connection;
            Address currentEndPoint = tcpConnection.getEndPoint();
            if (currentEndPoint != null && !currentEndPoint.equals(remoteEndPoint)) {
                throw new IllegalArgumentException(connection + " has already a different endpoint than: "
                        + remoteEndPoint);
            }
            tcpConnection.setEndPoint(remoteEndPoint);

            if (!connection.isClient()) {
                TcpIpConnectionMonitor connectionMonitor = getConnectionMonitor(remoteEndPoint, true);
                tcpConnection.setMonitor(connectionMonitor);
            }
        }
        connectionsMap.put(remoteEndPoint, connection);
        connectionsInProgress.remove(remoteEndPoint);
        ioService.getEventService().executeEventCallback(new StripedRunnable() {
            @Override
            public void run() {
                for (ConnectionListener listener : connectionListeners) {
                    listener.connectionAdded(connection);
                }
            }

            @Override
            public int getKey() {
                return remoteEndPoint.hashCode();
            }
        });
        return true;
    }

    private boolean checkAlreadyConnected(TcpIpConnection connection, Address remoteEndPoint) {
        final Connection existingConnection = connectionsMap.get(remoteEndPoint);
        if (existingConnection != null && existingConnection.isAlive()) {
            if (existingConnection != connection) {
                if (logger.isFinestEnabled()) {
                    logger.finest(existingConnection + " is already bound to " + remoteEndPoint
                            + ", new one is " + connection);
                }
                activeConnections.add(connection);
            }
            return true;
        }
        return false;
    }

    void sendBindRequest(TcpIpConnection connection, Address remoteEndPoint, boolean replyBack) {
        connection.setEndPoint(remoteEndPoint);
        ioService.onSuccessfulConnection(remoteEndPoint);
        //make sure bind packet is the first packet sent to the end point.
        if (logger.isFinestEnabled()) {
            logger.finest("Sending bind packet to " + remoteEndPoint);
        }
        BindMessage bind = new BindMessage(ioService.getThisAddress(), remoteEndPoint, replyBack);
        byte[] bytes = ioService.getSerializationService().toBytes(bind);
        Packet packet = new Packet(bytes);
        packet.setHeader(Packet.HEADER_BIND);
        connection.write(packet);
        //now you can send anything...
    }

    SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        SocketChannelWrapper wrapper = socketChannelWrapperFactory.wrapSocketChannel(socketChannel, client);
        acceptedSockets.add(wrapper);
        return wrapper;
    }

    TcpIpConnection newConnection(SocketChannelWrapper channel, Address endpoint) {
        TcpIpConnection connection = new TcpIpConnection(
                this,
                connectionIdGen.incrementAndGet(),
                channel,
                ioThreadingModel);

        connection.setEndPoint(endpoint);
        activeConnections.add(connection);
        acceptedSockets.remove(channel);

        connection.start();
        ioThreadingModel.onConnectionAdded(connection);

        logger.info("Established socket connection between "
                + channel.socket().getLocalSocketAddress() + " and " + channel.socket().getRemoteSocketAddress());
        openedCount.inc();

        return connection;
    }

    void failedConnection(Address address, Throwable t, boolean silent) {
        connectionsInProgress.remove(address);
        ioService.onFailedConnection(address);
        if (!silent) {
            getConnectionMonitor(address, false).onError(t);
        }
    }

    @Override
    public Connection getConnection(Address address) {
        return connectionsMap.get(address);
    }

    @Override
    public Connection getOrConnect(Address address) {
        return getOrConnect(address, false);
    }

    @Override
    public Connection getOrConnect(final Address address, final boolean silent) {
        Connection connection = connectionsMap.get(address);
        if (connection == null && live) {
            if (connectionsInProgress.add(address)) {
                ioService.shouldConnectTo(address);
                ioService.executeAsync(new InitConnectionTask(this, address, silent));
            }
        }
        return connection;
    }

    private TcpIpConnectionMonitor getConnectionMonitor(Address endpoint, boolean reset) {
        TcpIpConnectionMonitor monitor = ConcurrencyUtil.getOrPutIfAbsent(monitors, endpoint, monitorConstructor);
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    @Override
    public void destroyConnection(final Connection connection) {
        if (connection == null) {
            return;
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Destroying " + connection);
        }

        activeConnections.remove(connection);
        final Address endPoint = connection.getEndPoint();
        if (endPoint != null) {
            connectionsInProgress.remove(endPoint);
            connectionsMap.remove(endPoint, connection);
            // this should not be needed; but some tests are using DroppingConnection which is not a TcpIpConnection.
            if (connection instanceof TcpIpConnection) {
                ioThreadingModel.onConnectionRemoved((TcpIpConnection) connection);
            }
            if (live) {
                ioService.getEventService().executeEventCallback(new StripedRunnable() {
                    @Override
                    public void run() {
                        for (ConnectionListener listener : connectionListeners) {
                            listener.connectionRemoved(connection);
                        }
                    }

                    @Override
                    public int getKey() {
                        return endPoint.hashCode();
                    }
                });
            }
        }
        if (connection.isAlive()) {
            connection.close();
            closedCount.inc();
        }
    }

    protected void initSocket(Socket socket) throws Exception {
        if (ioService.getSocketLingerSeconds() > 0) {
            socket.setSoLinger(true, ioService.getSocketLingerSeconds());
        }
        socket.setKeepAlive(ioService.getSocketKeepAlive());
        socket.setTcpNoDelay(ioService.getSocketNoDelay());
        socket.setReceiveBufferSize(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
        socket.setSendBufferSize(ioService.getSocketSendBufferSize() * KILO_BYTE);
    }

    @Override
    public synchronized void start() {
        if (live) {
            return;
        }
        if (!serverSocketChannel.isOpen()) {
            throw new IllegalStateException("ConnectionManager is already shutdown. Cannot start!");
        }

        live = true;
        logger.finest("Starting ConnectionManager and IO selectors.");

        ioThreadingModel.start();
        startAcceptorThread();
    }

    private void startAcceptorThread() {
        if (acceptorThread != null) {
            logger.warning("SocketAcceptor thread is already live! Shutting down old acceptor...");
            acceptorThread.shutdown();
            acceptorThread = null;
        }

        acceptorThread = new SocketAcceptorThread(
                ioService.getThreadGroup(),
                ioService.getThreadPrefix() + "Acceptor",
                serverSocketChannel,
                this);
        acceptorThread.start();
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        live = false;
        logger.finest("Stopping ConnectionManager");

        if (acceptorThread != null) {
            acceptorThread.shutdown();
        }

        for (SocketChannelWrapper socketChannel : acceptedSockets) {
            closeResource(socketChannel);
        }
        for (Connection conn : connectionsMap.values()) {
            destroySilently(conn);
        }
        for (TcpIpConnection conn : activeConnections) {
            destroySilently(conn);
        }
        ioThreadingModel.shutdown();
        acceptedSockets.clear();
        connectionsInProgress.clear();
        connectionsMap.clear();
        monitors.clear();
        activeConnections.clear();
    }

    private void destroySilently(Connection conn) {
        try {
            destroyConnection(conn);
        } catch (Throwable ignore) {
            logger.finest(ignore);
        }
    }

    @Override
    public synchronized void shutdown() {
        if (acceptorThread != null) {
            acceptorThread.shutdown();
        }
        closeServerSocket();
        stop();
        connectionListeners.clear();
    }

    private void closeServerSocket() {
        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Closing server socket channel: " + serverSocketChannel);
            }
            serverSocketChannel.close();
        } catch (IOException ignore) {
            logger.finest(ignore);
        }
    }

    @Probe(name = "clientCount", level = MANDATORY)
    @Override
    public int getCurrentClientConnections() {
        int count = 0;
        for (TcpIpConnection conn : activeConnections) {
            if (conn.isAlive() && conn.isClient()) {
                count++;
            }
        }
        return count;
    }

    public boolean isLive() {
        return live;
    }

    boolean useAnyOutboundPort() {
        return outboundPortCount == 0;
    }

    int getOutboundPortCount() {
        return outboundPortCount;
    }

    int acquireOutboundPort() {
        if (useAnyOutboundPort()) {
            return 0;
        }
        synchronized (outboundPorts) {
            final Integer port = outboundPorts.removeFirst();
            outboundPorts.addLast(port);
            return port;
        }
    }


    @Override
    public boolean transmit(Packet packet, Connection connection) {
        checkNotNull(packet, "Packet can't be null");

        if (connection == null) {
            return false;
        }

        return connection.write(packet);
    }

    /**
     * Retries sending packet maximum 5 times until connection to target becomes available.
     */
    @Override
    public boolean transmit(Packet packet, Address target) {
        checkNotNull(packet, "Packet can't be null");
        checkNotNull(target, "target can't be null");

        return send(packet, target, null);
    }

    private boolean send(Packet packet, Address target, SendTask sendTask) {
        Connection connection = getConnection(target);
        if (connection != null) {
            return connection.write(packet);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target);
        }

        int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && ioService.isActive()) {
            getOrConnect(target, true);
            // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
            scheduler.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private final class SendTask implements Runnable {
        private final Packet packet;
        private final Address target;
        private volatile int retries;

        private SendTask(Packet packet, Address target) {
            this.packet = packet;
            this.target = target;
        }

        @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "single-writer, many-reader")
        @Override
        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Connections {");
        for (Connection conn : connectionsMap.values()) {
            sb.append("\n");
            sb.append(conn);
        }
        sb.append("\nlive=");
        sb.append(live);
        sb.append("\n}");
        return sb.toString();
    }
}

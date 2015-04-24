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

import com.hazelcast.cluster.impl.BindMessage;
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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.tcp.iobalancer.IOBalancer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.util.counters.MwCounter.newMwCounter;

public class TcpIpConnectionManager implements ConnectionManager {

    private static final int DEFAULT_KILL_THREAD_MILLIS = 1000 * 10;

    final int socketReceiveBufferSize;

    final IOService ioService;

    final int socketSendBufferSize;

    private final ConstructorFunction<Address, TcpIpConnectionMonitor> monitorConstructor
            = new ConstructorFunction<Address, TcpIpConnectionMonitor>() {
        public TcpIpConnectionMonitor createNew(Address endpoint) {
            return new TcpIpConnectionMonitor(TcpIpConnectionManager.this, endpoint);
        }
    };

    private final ILogger logger;

    private final int socketLingerSeconds;

    private final int socketConnectTimeoutSeconds;

    private final boolean socketKeepAlive;

    private final boolean socketNoDelay;

    @Probe(name = "count")
    private final ConcurrentHashMap<Address, Connection> connectionsMap = new ConcurrentHashMap<Address, Connection>(100);

    @Probe(name = "monitorCount")
    private final ConcurrentHashMap<Address, TcpIpConnectionMonitor> monitors =
            new ConcurrentHashMap<Address, TcpIpConnectionMonitor>(100);

    @Probe(name = "inProgressCount")
    private final Set<Address> connectionsInProgress =
            Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    @Probe(name = "connectionListenerCount")
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    @Probe(name = "acceptedSocketCount")
    private final Set<SocketChannelWrapper> acceptedSockets =
            Collections.newSetFromMap(new ConcurrentHashMap<SocketChannelWrapper, Boolean>());

    @Probe(name = "activeCount")
    private final Set<TcpIpConnection> activeConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<TcpIpConnection, Boolean>());

    @Probe(name = "textCount")
    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();
    private final MetricsRegistry metricRegistry;

    private volatile boolean live;

    private final ServerSocketChannel serverSocketChannel;

    private final int selectorThreadCount;

    private final InSelectorImpl[] inSelectors;

    private final OutSelectorImpl[] outSelectors;

    private final AtomicInteger nextSelectorIndex = new AtomicInteger();

    private final SocketChannelWrapperFactory socketChannelWrapperFactory;

    private final int outboundPortCount;

    private final HazelcastThreadGroup hazelcastThreadGroup;

    // accessed only in synchronized block
    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();

    // accessed only in synchronized block
    private volatile Thread socketAcceptorThread;

    private IOBalancer ioBalancer;
    private final LoggingService loggingService;

    @Probe
    private final MwCounter openedCount = newMwCounter();
    @Probe
    private final MwCounter closedCount = newMwCounter();

    public TcpIpConnectionManager(IOService ioService, ServerSocketChannel serverSocketChannel,
                                  HazelcastThreadGroup hazelcastThreadGroup, LoggingService loggingService) {
        this.ioService = ioService;
        this.metricRegistry = ioService.getMetricRegistry();
        this.hazelcastThreadGroup = hazelcastThreadGroup;
        this.serverSocketChannel = serverSocketChannel;
        this.logger = loggingService.getLogger(TcpIpConnectionManager.class.getName());
        this.socketReceiveBufferSize = ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE;
        this.socketSendBufferSize = ioService.getSocketSendBufferSize() * IOService.KILO_BYTE;
        this.socketLingerSeconds = ioService.getSocketLingerSeconds();
        this.socketConnectTimeoutSeconds = ioService.getSocketConnectTimeoutSeconds();
        this.socketKeepAlive = ioService.getSocketKeepAlive();
        this.socketNoDelay = ioService.getSocketNoDelay();
        this.selectorThreadCount = ioService.getSelectorThreadCount();
        this.inSelectors = new InSelectorImpl[selectorThreadCount];
        this.outSelectors = new OutSelectorImpl[selectorThreadCount];
        final Collection<Integer> ports = ioService.getOutboundPorts();
        this.outboundPortCount = ports.size();
        this.outboundPorts.addAll(ports);
        this.socketChannelWrapperFactory = ioService.getSocketChannelWrapperFactory();
        this.loggingService = loggingService;

        metricRegistry.scanAndRegister(this, "tcp.connection");
    }

    public MetricsRegistry getMetricRegistry() {
        return metricRegistry;
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

    public PacketReader createPacketReader(TcpIpConnection connection) {
        return ioService.createPacketReader(connection);
    }

    public PacketWriter createPacketWriter(TcpIpConnection connection) {
        return ioService.createPacketWriter(connection);
    }

    // just for testing
    public Set<TcpIpConnection> getActiveConnections() {
        return activeConnections;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public InSelectorImpl[] getInSelectors() {
        return inSelectors;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public OutSelectorImpl[] getOutSelectors() {
        return outSelectors;
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

    public IOBalancer getIOBalancer() {
        return ioBalancer;
    }

    public boolean isSSLEnabled() {
        return socketChannelWrapperFactory.isSSlEnabled();
    }

    public void incrementTextConnections() {
        allTextConnections.incrementAndGet();
    }

    public IOService getIOHandler() {
        return ioService;
    }

    public int getSocketConnectTimeoutSeconds() {
        return socketConnectTimeoutSeconds;
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public boolean bind(TcpIpConnection connection, Address remoteEndPoint, Address localEndpoint, boolean reply) {
        if (logger.isFinestEnabled()) {
            log(Level.FINEST, "Binding " + connection + " to " + remoteEndPoint + ", reply is " + reply);
        }
        final Address thisAddress = ioService.getThisAddress();
        if (ioService.isSocketBindAny() && !connection.isClient() && !thisAddress.equals(localEndpoint)) {
            log(Level.WARNING, "Wrong bind request from " + remoteEndPoint
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
        if (!registerConnection(remoteEndPoint, connection)) {
            return false;
        }
        return true;
    }

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
                    log(Level.FINEST, existingConnection + " is already bound to " + remoteEndPoint
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
            log(Level.FINEST, "Sending bind packet to " + remoteEndPoint);
        }
        BindMessage bind = new BindMessage(ioService.getThisAddress(), remoteEndPoint, replyBack);
        Data bindData = ioService.toData(bind);
        Packet packet = new Packet(bindData);
        packet.setHeader(Packet.HEADER_BIND);
        connection.write(packet);
        //now you can send anything...
    }

    private int nextSelectorIndex() {
        return Math.abs(nextSelectorIndex.getAndIncrement()) % selectorThreadCount;
    }

    SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        SocketChannelWrapper wrapper = socketChannelWrapperFactory.wrapSocketChannel(socketChannel, client);
        acceptedSockets.add(wrapper);
        return wrapper;
    }

    TcpIpConnection assignSocketChannel(SocketChannelWrapper channel, Address endpoint) {
        int index = nextSelectorIndex();

        final TcpIpConnection connection = new TcpIpConnection(this, inSelectors[index],
                outSelectors[index], connectionIdGen.incrementAndGet(), channel);

        connection.setEndPoint(endpoint);
        activeConnections.add(connection);
        acceptedSockets.remove(channel);

        connection.start();
        ioBalancer.connectionAdded(connection);

        log(Level.INFO, "Established socket connection between " + channel.socket().getLocalSocketAddress());
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
                ioService.executeAsync(new SocketConnector(this, address, silent));
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
            log(Level.FINEST, "Destroying " + connection);
        }

        activeConnections.remove(connection);
        final Address endPoint = connection.getEndPoint();
        if (endPoint != null) {
            connectionsInProgress.remove(endPoint);
            connectionsMap.remove(endPoint, connection);
            ioBalancer.connectionRemoved(connection);
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
        if (socketLingerSeconds > 0) {
            socket.setSoLinger(true, socketLingerSeconds);
        }
        socket.setKeepAlive(socketKeepAlive);
        socket.setTcpNoDelay(socketNoDelay);
        socket.setReceiveBufferSize(socketReceiveBufferSize);
        socket.setSendBufferSize(socketSendBufferSize);
    }

    @Override
    public synchronized void start() {
        if (live) {
            return;
        }
        live = true;
        log(Level.FINEST, "Starting ConnectionManager and IO selectors.");
        IOSelectorOutOfMemoryHandler oomeHandler = new IOSelectorOutOfMemoryHandler() {
            @Override
            public void handle(OutOfMemoryError error) {
                ioService.onOutOfMemory(error);
            }
        };
        for (int i = 0; i < inSelectors.length; i++) {
            InSelectorImpl inSelector = new InSelectorImpl(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "in-" + i,
                    ioService.getLogger(InSelectorImpl.class.getName()),
                    oomeHandler
            );
            inSelectors[i] = inSelector;
            metricRegistry.scanAndRegister(inSelector, "tcp." + inSelector.getName());
            inSelector.start();

            OutSelectorImpl outSelector = new OutSelectorImpl(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "out-" + i,
                    ioService.getLogger(OutSelectorImpl.class.getName()),
                    oomeHandler);
            outSelectors[i] = outSelector;
            metricRegistry.scanAndRegister(outSelector, "tcp." + outSelector.getName());
            outSelector.start();
        }
        startIOBalancer();

        if (socketAcceptorThread != null) {
            logger.warning("SocketAcceptor thread is already live! Shutting down old acceptor...");
            shutdownSocketAcceptor();
        }
        Runnable acceptRunnable = new SocketAcceptor(serverSocketChannel, this);
        socketAcceptorThread = new Thread(ioService.getThreadGroup(), acceptRunnable,
                ioService.getThreadPrefix() + "Acceptor");
        socketAcceptorThread.start();
    }

    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inSelectors, outSelectors,
                hazelcastThreadGroup, ioService.getBalancerIntervalSeconds(), loggingService);
        ioBalancer.start();
        metricRegistry.scanAndRegister(ioBalancer, "tcp.balancer");
    }

    @Override
    public synchronized void restart() {
        stop();
        start();
    }

    @Override
    public synchronized void shutdown() {
        if (!live) {
            return;
        }
        live = false;
        shutdownSocketAcceptor();
        closeServerSocket();
        stop();
        connectionListeners.clear();
    }

    private void closeServerSocket() {
        try {
            if (logger.isFinestEnabled()) {
                log(Level.FINEST, "Closing server socket channel: " + serverSocketChannel);
            }
            serverSocketChannel.close();
        } catch (IOException ignore) {
            logger.finest(ignore);
        }
    }

    private void stop() {
        live = false;
        log(Level.FINEST, "Stopping ConnectionManager");
        ioBalancer.stop();
        shutdownSocketAcceptor();
        for (SocketChannelWrapper socketChannel : acceptedSockets) {
            IOUtil.closeResource(socketChannel);
        }
        for (Connection conn : connectionsMap.values()) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.finest(ignore);
            }
        }
        for (TcpIpConnection conn : activeConnections) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.finest(ignore);
            }
        }
        shutdownIOSelectors();
        acceptedSockets.clear();
        connectionsInProgress.clear();
        connectionsMap.clear();
        monitors.clear();
        activeConnections.clear();
    }

    private synchronized void shutdownIOSelectors() {
        if (logger.isFinestEnabled()) {
            log(Level.FINEST, "Shutting down IO selectors... Total: " + selectorThreadCount);
        }
        for (int i = 0; i < selectorThreadCount; i++) {
            IOSelector ioSelector = inSelectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            inSelectors[i] = null;

            ioSelector = outSelectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            outSelectors[i] = null;
        }
    }

    private void shutdownSocketAcceptor() {
        log(Level.FINEST, "Shutting down SocketAcceptor thread.");
        Thread killingThread = socketAcceptorThread;
        if (killingThread == null) {
            return;
        }
        socketAcceptorThread = null;
        killingThread.interrupt();
        try {
            killingThread.join(DEFAULT_KILL_THREAD_MILLIS);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
    }

    @Probe(name = "clientCount")
    @Override
    public int getCurrentClientConnections() {
        int count = 0;
        for (TcpIpConnection conn : activeConnections) {
            if (conn.isAlive()) {
                if (conn.isClient()) {
                    count++;
                }
            }
        }
        return count;
    }

    public boolean isLive() {
        return live;
    }

    private void log(Level level, String message) {
        logger.log(level, message);
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

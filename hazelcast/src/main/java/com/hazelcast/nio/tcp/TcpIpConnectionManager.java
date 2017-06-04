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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.cluster.impl.BindMessage;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.StripedRunnable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

public class TcpIpConnectionManager implements ConnectionManager, PacketHandler {

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;
    private static final int SCHEDULER_POOL_SIZE = 4;

    // TODO Introducing this to allow disabling the spoofing checks on-demand
    // if there is a use-case that gets affected by the change. If there are no reports of misbehaviour we can remove than in
    // next release.
    private static final boolean SPOOFING_CHECKS = parseBoolean(getProperty("hazelcast.nio.tcp.spoofing.checks", "true"));

    final LoggingService loggingService;

    @Probe(name = "connectionListenerCount")
    final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final IOService ioService;

    private final ConstructorFunction<Address, TcpIpConnectionErrorHandler> monitorConstructor
            = new ConstructorFunction<Address, TcpIpConnectionErrorHandler>() {
        public TcpIpConnectionErrorHandler createNew(Address endpoint) {
            return new TcpIpConnectionErrorHandler(TcpIpConnectionManager.this, endpoint);
        }
    };

    private final ILogger logger;

    @Probe(name = "count", level = MANDATORY)
    private final ConcurrentHashMap<Address, Connection> connectionsMap = new ConcurrentHashMap<Address, Connection>(100);

    @Probe(name = "monitorCount")
    private final ConcurrentHashMap<Address, TcpIpConnectionErrorHandler> monitors =
            new ConcurrentHashMap<Address, TcpIpConnectionErrorHandler>(100);

    @Probe(name = "inProgressCount")
    private final Set<Address> connectionsInProgress =
            Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    @Probe(name = "acceptedSocketCount", level = MANDATORY)
    private final Set<Channel> acceptedSockets =
            Collections.newSetFromMap(new ConcurrentHashMap<Channel, Boolean>());

    @Probe(name = "activeCount", level = MANDATORY)
    private final Set<TcpIpConnection> activeConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<TcpIpConnection, Boolean>());

    @Probe(name = "textCount", level = MANDATORY)
    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private final EventLoopGroup eventLoopGroup;
    private final MetricsRegistry metricsRegistry;

    private final ServerSocketChannel serverSocketChannel;

    private final ChannelFactory channelFactory;
    @Probe
    private final MwCounter openedCount = newMwCounter();
    @Probe
    private final MwCounter closedCount = newMwCounter();

    private final ScheduledExecutorService scheduler;

    // accessed only in synchronized block
    private volatile TcpIpAcceptor acceptor;

    private volatile boolean live;

    private TcpIpConnector connector;

    public TcpIpConnectionManager(IOService ioService,
                                  ServerSocketChannel serverSocketChannel,
                                  LoggingService loggingService,
                                  MetricsRegistry metricsRegistry,
                                  EventLoopGroup eventLoopGroup) {
        this.ioService = ioService;
        this.eventLoopGroup = eventLoopGroup;
        this.serverSocketChannel = serverSocketChannel;
        this.loggingService = loggingService;
        this.logger = loggingService.getLogger(TcpIpConnectionManager.class);
        this.channelFactory = ioService.getChannelFactory();
        this.metricsRegistry = metricsRegistry;
        this.connector = new TcpIpConnector(this);
        this.scheduler = new ScheduledThreadPoolExecutor(SCHEDULER_POOL_SIZE,
                new ThreadFactoryImpl(createThreadPoolName(ioService.getHazelcastName(), "TcpIpConnectionManager")));
        metricsRegistry.scanAndRegister(this, "tcp.connection");
    }

    public IOService getIoService() {
        return ioService;
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
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

    public void incrementTextConnections() {
        allTextConnections.incrementAndGet();
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        checkNotNull(listener, "listener can't be null");
        connectionListeners.add(listener);
    }

    private final ConcurrentMap<String, List<TcpIpConnection>> tmpConnections = new ConcurrentHashMap<String, List<TcpIpConnection>>();
    private final ConcurrentMap<TcpIpConnection, BindMessage> bindMessages = new ConcurrentHashMap<TcpIpConnection, BindMessage>();

    @Override
    public void handle(Packet packet) throws Exception {
        assert packet.getPacketType() == Packet.Type.BIND;

        TcpIpConnection connection = (TcpIpConnection) packet.getConn();
        BindMessage bind = ioService.getSerializationService().toObject(packet);
        bindMessages.put(connection, bind);

        logger.info("receiving bind:" + bind);

        String key = bind.getLocalAddress() + "|" + bind.getTargetAddress();
        List<TcpIpConnection> connections = tmpConnections.get(key);
        if (connections == null) {
            List<TcpIpConnection> newConnections = new ArrayList<TcpIpConnection>();
            List<TcpIpConnection> foundConnections = tmpConnections.putIfAbsent(key, newConnections);
            connections = foundConnections == null ? newConnections : foundConnections;
        }

        boolean register;
        TcpIpConnection targetConnection = null;
        synchronized (connections) {
            connections.add((TcpIpConnection) packet.getConn());
            register = connections.size() == ioService.supportChannelCount() + 1;

            for (TcpIpConnection c : connections) {
                BindMessage bindMessage = bindMessages.get(c);
                logger.info("bindMessage:" + bindMessage);
                if (bindMessage.getChannelIndex() == 0) {
                    targetConnection = c;
                    break;
                }
            }
        }

        if (bind(connection, bind.getLocalAddress(), bind.getTargetAddress(), bind.shouldReply(), bind.getChannelIndex())) {
            logger.info("Bind completes;" + bind);
        } else {
            logger.info("Bind ignored: " + bind);
        }

        if (register) {
            registerConnection(bind.getLocalAddress(), targetConnection);
            logger.info("Registering");
        } else {
            logger.info("Skipping registration");
        }
    }

    /**
     * Binding completes the connection and makes it available to be used with the ConnectionManager.
     */
    private synchronized boolean bind(
            TcpIpConnection connection, Address remoteEndPoint, Address localEndpoint, boolean reply, int channelIndex) {

        if (logger.isFinestEnabled()) {
            logger.finest("Binding " + connection + " to " + remoteEndPoint + ", reply is " + reply);
        }

        final Address thisAddress = ioService.getThisAddress();
//        if (ioService.isSocketBindAny() && !connection.isClient() && !thisAddress.equals(localEndpoint)) {
//            String msg = "Wrong bind request from " + remoteEndPoint
//                    + "! This node is not requested endpoint: " + localEndpoint;
//            logger.warning(msg);
//            connection.close(msg, null);
//            return false;
//        }

        connection.setEndPoint(remoteEndPoint);

        if (channelIndex == 0) {
            ioService.onSuccessfulConnection(remoteEndPoint);
        }

        if (reply) {
            sendBindRequest(connection, remoteEndPoint, false, channelIndex);
        }

        return true;
    }

    private boolean ensureValidBindSource(TcpIpConnection connection, Address remoteEndPoint) {
        try {
            InetAddress originalRemoteAddr = connection.getRemoteSocketAddress().getAddress();
            InetAddress presentedRemoteAddr = remoteEndPoint.getInetAddress();
            if (!originalRemoteAddr.equals(presentedRemoteAddr)) {
                String msg =  "Wrong bind request from " + originalRemoteAddr + ", identified as " + presentedRemoteAddr;
                logger.warning(msg);
                connection.close(msg, null);
                return false;
            }
        } catch (UnknownHostException e) {
            String msg = e.getMessage();
            logger.warning(msg);
            connection.close(msg, e);
            return false;
        }
        return true;
    }

    private boolean ensureValidBindTarget(TcpIpConnection connection, Address remoteEndPoint, Address localEndpoint,
                                          Address thisAddress) {
        if (ioService.isSocketBindAny() && !connection.isClient() && !thisAddress.equals(localEndpoint)) {
            String msg = "Wrong bind request from " + remoteEndPoint
                       + "! This node is not the requested endpoint: " + localEndpoint;
            logger.warning(msg);
            connection.close(msg, null);
            return false;
        }
        return true;
    }

    private boolean ensureBindNotFromSelf(TcpIpConnection connection, Address remoteEndPoint, Address thisAddress) {
        if (thisAddress.equals(remoteEndPoint)) {
            String msg = "Wrong bind request. Remote endpoint is same to this endpoint.";
            logger.warning(msg);
            connection.close(msg, null);
            return false;
        }
        return true;
    }

    @Override
    public synchronized boolean registerConnection(final Address remoteEndPoint,
                                                   final Connection connection) {
        try {
            if (remoteEndPoint.equals(ioService.getThisAddress())) {
                return false;
            }

            if (!connection.isAlive()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(connection + " to " + remoteEndPoint + " is not registered since connection is not active.");
                }
                return false;
            }

            if (connection instanceof TcpIpConnection) {
                TcpIpConnection tcpConnection = (TcpIpConnection) connection;
                BindMessage bind = bindMessages.get(tcpConnection);
                String key = bind.getLocalAddress() + "|" + bind.getTargetAddress();
                List<TcpIpConnection> connections = tmpConnections.get(key);

                Channel[] channels = new Channel[ioService.supportChannelCount() + 1];
                for (TcpIpConnection c : connections) {
                    BindMessage bindMessage = bindMessages.get(c);
                    channels[bindMessage.getChannelIndex()] = c.getChannel();
                }

                tcpConnection.updateChannels(channels);

                Address currentEndPoint = tcpConnection.getEndPoint();
                if (currentEndPoint != null && !currentEndPoint.equals(remoteEndPoint)) {
                    throw new IllegalArgumentException(connection + " has already a different endpoint than: "
                            + remoteEndPoint);
                }
                tcpConnection.setEndPoint(remoteEndPoint);

                if (!connection.isClient()) {
                    TcpIpConnectionErrorHandler connectionMonitor = getErrorHandler(remoteEndPoint, true);
                    tcpConnection.setErrorHandler(connectionMonitor);
                }
            }
            connectionsMap.put(remoteEndPoint, connection);

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
        } finally {
            connectionsInProgress.remove(remoteEndPoint);
        }
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

    void sendBindRequest(TcpIpConnection connection, Address remoteEndPoint, boolean reply, int channelIndex) {
        connection.setEndPoint(remoteEndPoint);

        ioService.onSuccessfulConnection(remoteEndPoint);
        //make sure bind packet is the first packet sent to the end point.
        if (logger.isFinestEnabled()) {
            logger.finest("Sending bind packet to " + remoteEndPoint);
        }
        BindMessage bind = new BindMessage(ioService.getThisAddress(), remoteEndPoint, reply, channelIndex);
        byte[] bytes = ioService.getSerializationService().toBytes(bind);
        Packet packet = new Packet(bytes).setPacketType(Packet.Type.BIND);
        connection.write(packet);
        //now you can send anything...
    }

    Channel createChannel(SocketChannel socketChannel, boolean client) throws Exception {
        Channel wrapper = channelFactory.create(socketChannel, client, ioService.useDirectSocketBuffer());
        acceptedSockets.add(wrapper);
        return wrapper;
    }

    synchronized TcpIpConnection newConnection(Channel channel, Address endpoint) {
        try {
            if (!live) {
                throw new IllegalStateException("connection manager is not live!");
            }

            // todo: how to figure out the supporting channels that have been made
            // in theory any binding on the non primary interface could be seen as primary
            // and with the
            TcpIpConnection connection = new TcpIpConnection(this, connectionIdGen.incrementAndGet(), channel);

            connection.setEndPoint(endpoint);
            activeConnections.add(connection);

            logger.info("Established socket connection between "
                    + channel.getLocalSocketAddress() + " and " + channel.getRemoteSocketAddress());
            openedCount.inc();

            eventLoopGroup.register(channel);
            return connection;
        } finally {
            acceptedSockets.remove(channel);
        }
    }

    void failedConnection(Address address, Throwable t, boolean silent) {
        connectionsInProgress.remove(address);
        ioService.onFailedConnection(address);
        if (!silent) {
            getErrorHandler(address, false).onError(t);
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
                connector.asyncConnect(address, silent);
            }
        }
        return connection;
    }

    private TcpIpConnectionErrorHandler getErrorHandler(Address endpoint, boolean reset) {
        TcpIpConnectionErrorHandler monitor = ConcurrencyUtil.getOrPutIfAbsent(monitors, endpoint, monitorConstructor);
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    /**
     * Deals with cleaning up a closed connection. This method should only be called once by the
     * {@link TcpIpConnection#close(String, Throwable)} method where it is protected against multiple closes.
     */
    @Override
    public void onConnectionClose(Connection connection) {
        closedCount.inc();

        activeConnections.remove(connection);

        Address endPoint = connection.getEndPoint();
        if (endPoint != null) {
            connectionsInProgress.remove(endPoint);
            connectionsMap.remove(endPoint, connection);
            fireConnectionRemovedEvent(connection, endPoint);
        }
    }

    private void fireConnectionRemovedEvent(final Connection connection, final Address endPoint) {
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

        eventLoopGroup.start();
        startAcceptor();
    }

    private void startAcceptor() {
        if (acceptor != null) {
            logger.warning("TcpIpAcceptor already is running! Shutting down old acceptor...");
            shutdownAcceptor();
        }

        acceptor = new TcpIpAcceptor(serverSocketChannel, this).start();
        metricsRegistry.collectMetrics(acceptor);
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        live = false;
        logger.finest("Stopping ConnectionManager");

        shutdownAcceptor();

        for (Channel socketChannel : acceptedSockets) {
            closeResource(socketChannel);
        }
        for (Connection conn : connectionsMap.values()) {
            destroySilently(conn, "TcpIpConnectionManager is stopping");
        }
        for (TcpIpConnection conn : activeConnections) {
            destroySilently(conn, "TcpIpConnectionManager is stopping");
        }
        eventLoopGroup.shutdown();
        acceptedSockets.clear();
        connectionsInProgress.clear();
        connectionsMap.clear();
        monitors.clear();
        activeConnections.clear();
    }

    private void destroySilently(Connection conn, String reason) {
        if (conn == null) {
            return;
        }

        try {
            conn.close(reason, null);
        } catch (Throwable ignore) {
            logger.finest(ignore);
        }
    }

    @Override
    public synchronized void shutdown() {
        shutdownAcceptor();
        closeServerSocket();
        stop();
        scheduler.shutdownNow();
        connectionListeners.clear();
    }

    private void shutdownAcceptor() {
        if (acceptor != null) {
            acceptor.shutdown();
            metricsRegistry.deregister(acceptor);
            acceptor = null;
        }
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
            try {
                scheduler.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                return true;
            } catch (RejectedExecutionException e) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Packet send task is rejected. Packet cannot be sent to " + target);
                }
            }
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

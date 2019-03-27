/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionLifecycleListener;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.EndpointManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.NetworkingService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.function.Consumer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.nio.IOUtil.close;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.setChannelOptions;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

public class TcpIpEndpointManager
        implements EndpointManager<TcpIpConnection>, Consumer<Packet> {

    private static final int RETRY_NUMBER = 5;
    private static final long DELAY_FACTOR = 100L;

    @Probe(name = "inProgressCount")
    final Set<Address> connectionsInProgress = newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    @Probe(name = "count", level = MANDATORY)
    final ConcurrentHashMap<Address, TcpIpConnection> connectionsMap =
            new ConcurrentHashMap<Address, TcpIpConnection>(100);

    @Probe(name = "activeCount", level = MANDATORY)
    final Set<TcpIpConnection> activeConnections = newSetFromMap(new ConcurrentHashMap<TcpIpConnection, Boolean>());

    private final ILogger logger;
    private final IOService ioService;
    private final EndpointConfig endpointConfig;
    private final EndpointQualifier endpointQualifier;
    private final ChannelInitializerProvider channelInitializerProvider;
    private final NetworkingService networkingService;
    private final TcpIpConnector connector;
    private final BindHandler bindHandler;

    @Probe(name = "connectionListenerCount")
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final ConstructorFunction<Address, TcpIpConnectionErrorHandler> monitorConstructor =
            new ConstructorFunction<Address, TcpIpConnectionErrorHandler>() {
                public TcpIpConnectionErrorHandler createNew(Address endpoint) {
                    return new TcpIpConnectionErrorHandler(TcpIpEndpointManager.this, endpoint);
                }
            };

    @Probe(name = "monitorCount")
    private final ConcurrentHashMap<Address, TcpIpConnectionErrorHandler> monitors =
            new ConcurrentHashMap<Address, TcpIpConnectionErrorHandler>(100);


    private final AtomicInteger connectionIdGen = new AtomicInteger();

    @Probe
    private final MwCounter openedCount = newMwCounter();

    @Probe
    private final MwCounter closedCount = newMwCounter();

    @Probe(name = "acceptedSocketCount", level = MANDATORY)
    private final Set<Channel> acceptedChannels = newSetFromMap(new ConcurrentHashMap<Channel, Boolean>());

    private final EndpointConnectionLifecycleListener connectionLifecycleListener = new EndpointConnectionLifecycleListener();

    TcpIpEndpointManager(NetworkingService networkingService, EndpointConfig endpointConfig,
                         ChannelInitializerProvider channelInitializerProvider, IOService ioService,
                         LoggingService loggingService, MetricsRegistry metricsRegistry,
                         HazelcastProperties properties, Set<ProtocolType> supportedProtocolTypes) {
        this.networkingService = networkingService;
        this.endpointConfig = endpointConfig;
        this.endpointQualifier = endpointConfig != null ? endpointConfig.getQualifier() : null;
        this.channelInitializerProvider = channelInitializerProvider;
        this.ioService = ioService;
        this.logger = loggingService.getLogger(TcpIpEndpointManager.class);
        this.connector = new TcpIpConnector(this);

        boolean spoofingChecks = properties != null && properties.getBoolean(GroupProperty.BIND_SPOOFING_CHECKS);
        this.bindHandler = new BindHandler(this, ioService, logger, spoofingChecks, supportedProtocolTypes);

        if (endpointQualifier == null) {
            metricsRegistry.scanAndRegister(this, "tcp.connection");
        } else {
            metricsRegistry.scanAndRegister(this, endpointQualifier.toMetricsPrefixString() + ".tcp.connection");
        }
    }

    public NetworkingService getNetworkingService() {
        return networkingService;
    }

    public EndpointQualifier getEndpointQualifier() {
        return endpointQualifier;
    }

    public Collection<TcpIpConnection> getActiveConnections() {
        return unmodifiableSet(activeConnections);
    }

    public Collection<TcpIpConnection> getConnections() {
        return unmodifiableCollection(new HashSet<TcpIpConnection>(connectionsMap.values()));
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        checkNotNull(listener, "listener can't be null");
        connectionListeners.add(listener);
    }

    @Override
    public synchronized void accept(Packet packet) {
        bindHandler.process(packet);
    }

    @Override
    public TcpIpConnection getConnection(Address address) {
        return connectionsMap.get(address);
    }

    @Override
    public TcpIpConnection getOrConnect(Address address) {
        return getOrConnect(address, false);
    }

    @Override
    public TcpIpConnection getOrConnect(final Address address, final boolean silent) {
        TcpIpConnection connection = connectionsMap.get(address);
        if (connection == null && networkingService.isLive()) {
            if (connectionsInProgress.add(address)) {
                connector.asyncConnect(address, silent);
            }
        }
        return connection;
    }

    @Override
    public synchronized boolean registerConnection(final Address remoteEndPoint, final TcpIpConnection connection) {
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

            Address currentEndPoint = connection.getEndPoint();
            if (currentEndPoint != null && !currentEndPoint.equals(remoteEndPoint)) {
                throw new IllegalArgumentException(connection + " has already a different endpoint than: " + remoteEndPoint);
            }
            connection.setEndPoint(remoteEndPoint);

            if (!connection.isClient()) {
                TcpIpConnectionErrorHandler connectionMonitor = getErrorHandler(remoteEndPoint, true);
                connection.setErrorHandler(connectionMonitor);
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

    private void fireConnectionRemovedEvent(final Connection connection, final Address endPoint) {
        if (networkingService.isLive()) {
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

    public synchronized void reset(boolean cleanListeners) {
        for (Channel socketChannel : acceptedChannels) {
            closeResource(socketChannel);
        }
        for (Connection conn : connectionsMap.values()) {
            close(conn, "EndpointManager is stopping");
        }
        for (Connection conn : activeConnections) {
            close(conn, "EndpointManager is stopping");
        }
        acceptedChannels.clear();
        connectionsInProgress.clear();
        connectionsMap.clear();
        monitors.clear();
        activeConnections.clear();

        if (cleanListeners) {
            connectionListeners.clear();
        }
    }

    @Override
    public boolean transmit(Packet packet, TcpIpConnection connection) {
        checkNotNull(packet, "Packet can't be null");

        if (connection == null) {
            return false;
        }

        return connection.write(packet);
    }

    @Override
    public boolean transmit(Packet packet, Address target) {
        checkNotNull(packet, "Packet can't be null");
        checkNotNull(target, "target can't be null");

        return send(packet, target, null);
    }

    private TcpIpConnectionErrorHandler getErrorHandler(Address endpoint, boolean reset) {
        TcpIpConnectionErrorHandler monitor = ConcurrencyUtil.getOrPutIfAbsent(monitors, endpoint, monitorConstructor);
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    Channel newChannel(SocketChannel socketChannel, boolean clientMode)
            throws IOException {
        Networking networking = getNetworkingService().getNetworking();
        Channel channel = networking.register(endpointQualifier, channelInitializerProvider, socketChannel, clientMode);
        // Advanced Network
        if (endpointConfig != null) {
            setChannelOptions(channel, endpointConfig);
        }
        acceptedChannels.add(channel);
        return channel;
    }

    void removeAcceptedChannel(Channel channel) {
        acceptedChannels.remove(channel);
    }

    void failedConnection(Address address, Throwable t, boolean silent) {
        connectionsInProgress.remove(address);
        ioService.onFailedConnection(address);
        if (!silent) {
            getErrorHandler(address, false).onError(t);
        }
    }

    synchronized TcpIpConnection newConnection(Channel channel, Address endpoint) {
        try {
            if (!networkingService.isLive()) {
                throw new IllegalStateException("connection manager is not live!");
            }

            TcpIpConnection connection = new TcpIpConnection(this, connectionLifecycleListener,
                    connectionIdGen.incrementAndGet(), channel);

            connection.setEndPoint(endpoint);
            activeConnections.add(connection);

            if (logger.isFineEnabled()) {
                logger.fine("Established socket connection between " + channel.localSocketAddress() + " and " + channel
                        .remoteSocketAddress());
            }
            openedCount.inc();

            channel.start();

            return connection;
        } finally {
            acceptedChannels.remove(channel);
        }
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
                networkingService.scheduleDeferred(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                return true;
            } catch (RejectedExecutionException e) {
                if (networkingService.isLive()) {
                    throw e;
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Packet send task is rejected. Packet cannot be sent to " + target);
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "TcpIpEndpointManager{" + "endpointQualifier=" + endpointQualifier + ", connectionsMap=" + connectionsMap + '}';
    }

    // test support
    int getAcceptedChannelsSize() {
        return acceptedChannels.size();
    }

    // test support
    int getConnectionListenersCount() {
        return connectionListeners.size();
    }

    private final class SendTask
            implements Runnable {
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

    public final class EndpointConnectionLifecycleListener
            implements ConnectionLifecycleListener<TcpIpConnection> {

        @Override
        public void onConnectionClose(TcpIpConnection connection, Throwable t, boolean silent) {
            closedCount.inc();

            activeConnections.remove(connection);

            Address endPoint = connection.getEndPoint();
            if (endPoint != null) {
                connectionsInProgress.remove(endPoint);
                connectionsMap.remove(endPoint, connection);
                fireConnectionRemovedEvent(connection, endPoint);
            }

            if (t != null) {
                ioService.onFailedConnection(endPoint);
                if (!silent) {
                    getErrorHandler(endPoint, false).onError(t);
                }
            }
        }

    }

}

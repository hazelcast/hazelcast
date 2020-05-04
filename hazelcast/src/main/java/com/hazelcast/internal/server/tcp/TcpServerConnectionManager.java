/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.AbstractChannelInitializer.MemberHandshakeHandler;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_BINDADDRESS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_ENDPOINT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_CLIENT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_IN_PROGRESS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_MONITOR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_TEXT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_TAG_ENDPOINT;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.nio.ConnectionType.MEMCACHE_CLIENT;
import static com.hazelcast.internal.nio.ConnectionType.REST_CLIENT;
import static com.hazelcast.internal.nio.IOUtil.close;
import static com.hazelcast.internal.nio.IOUtil.setChannelOptions;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

@SuppressWarnings("checkstyle:methodcount")
public class TcpServerConnectionManager
        implements ServerConnectionManager, Consumer<Packet>, DynamicMetricsProvider {

    private static final int RETRY_NUMBER = 5;
    private static final long DELAY_FACTOR = 100L;

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_IN_PROGRESS_COUNT)
    final Set<Address> connectionsInProgress = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_COUNT, level = MANDATORY)
    final ConcurrentHashMap<Address, TcpServerConnection> mappedConnections = new ConcurrentHashMap<>(100);

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT, level = MANDATORY)
    final Set<TcpServerConnection> connections = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT, level = MANDATORY)
    final Set<Channel> acceptedChannels = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT)
    final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<>();

    private final ILogger logger;
    private final ServerContext serverContext;
    private final EndpointConfig endpointConfig;
    private final EndpointQualifier endpointQualifier;
    private final Function<EndpointQualifier, ChannelInitializer> channelInitializerFn;
    private final TcpServer server;
    private final TcpServerConnector connector;
    private final MemberHandshakeHandler memberHandshakeHandler;
    private final NetworkStatsImpl networkStats;
    private final ConstructorFunction<Address, TcpServerConnectionErrorHandler> errorHandlerConstructor =
            endpoint -> new TcpServerConnectionErrorHandler(TcpServerConnectionManager.this, endpoint);

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_MONITOR_COUNT)
    private final ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> monitors = new ConcurrentHashMap<>(100);

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT)
    private final MwCounter openedCount = newMwCounter();

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT)
    private final MwCounter closedCount = newMwCounter();

    private final ConnectionLifecycleListenerImpl connectionLifecycleListener = new ConnectionLifecycleListenerImpl();

    TcpServerConnectionManager(TcpServer server,
                               EndpointConfig endpointConfig,
                               Function<EndpointQualifier, ChannelInitializer> channelInitializerFn,
                               ServerContext serverContext,
                               Set<ProtocolType> supportedProtocolTypes) {
        this.server = server;
        this.endpointConfig = endpointConfig;
        this.endpointQualifier = endpointConfig != null ? endpointConfig.getQualifier() : null;
        this.channelInitializerFn = channelInitializerFn;
        this.serverContext = serverContext;
        this.logger = serverContext.getLoggingService().getLogger(TcpServerConnectionManager.class);
        this.connector = new TcpServerConnector(this);
        this.memberHandshakeHandler = new MemberHandshakeHandler(this, serverContext, logger, supportedProtocolTypes);
        this.networkStats = endpointQualifier == null ? null : new NetworkStatsImpl();
    }

    @Override
    public TcpServer getServer() {
        return server;
    }

    public EndpointQualifier getEndpointQualifier() {
        return endpointQualifier;
    }

    public Collection<ServerConnection> getActiveConnections() {
        return unmodifiableSet(connections);
    }

    @Override
    public int connectionCount(Predicate<ServerConnection> predicate) {
        if (predicate == null) {
            return connections.size();
        } else {
            return (int) connections.stream().filter(predicate).count();
        }
    }

    @Override
    public @Nonnull Collection<ServerConnection> getConnections() {
        return unmodifiableCollection(connections);
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        checkNotNull(listener, "listener can't be null");
        connectionListeners.add(listener);
    }

    @Override
    public synchronized void accept(Packet packet) {
        memberHandshakeHandler.process(packet);
    }

    @Override
    public ServerConnection get(Address address) {
        return mappedConnections.get(address);
    }

    @Override
    public ServerConnection getOrConnect(final Address address, final boolean silent) {
        TcpServerConnection connection = mappedConnections.get(address);
        if (connection == null && server.isLive()) {
            if (connectionsInProgress.add(address)) {
                connector.asyncConnect(address, silent);
            }
        }
        return connection;
    }

    @Override
    public synchronized boolean register(final Address remoteAddress, final ServerConnection c) {
        TcpServerConnection connection = (TcpServerConnection) c;
        try {
            if (remoteAddress.equals(serverContext.getThisAddress())) {
                return false;
            }

            if (!connection.isAlive()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(connection + " to " + remoteAddress + " is not registered since connection is not active.");
                }
                return false;
            }

            Address currentRemoteAddress = connection.getRemoteAddress();
            if (currentRemoteAddress != null && !currentRemoteAddress.equals(remoteAddress)) {
                throw new IllegalArgumentException(connection + " has already a different remoteAddress than: " + remoteAddress);
            }
            connection.setRemoteAddress(remoteAddress);

            if (!connection.isClient()) {
                connection.setErrorHandler(getErrorHandler(remoteAddress, true));
            }
            mappedConnections.put(remoteAddress, connection);

            serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    connectionListeners.forEach(listener -> listener.connectionAdded(connection));
                }

                @Override
                public int getKey() {
                    return remoteAddress.hashCode();
                }
            });
            return true;
        } finally {
            connectionsInProgress.remove(remoteAddress);
        }
    }

    private void fireConnectionRemovedEvent(final Connection connection, final Address endPoint) {
        if (server.isLive()) {
            serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    connectionListeners.forEach(listener -> listener.connectionRemoved(connection));
                }

                @Override
                public int getKey() {
                    return endPoint.hashCode();
                }
            });
        }
    }

    public synchronized void reset(boolean cleanListeners) {
        acceptedChannels.forEach(IOUtil::closeResource);
        mappedConnections.values().forEach(conn -> close(conn, "TcpServerConnectionManager is stopping"));
        connections.forEach(conn -> close(conn, "TcpServerConnectionManager is stopping"));
        acceptedChannels.clear();
        connectionsInProgress.clear();
        mappedConnections.clear();
        monitors.clear();
        connections.clear();

        if (cleanListeners) {
            connectionListeners.clear();
        }
    }

    @Override
    public boolean transmit(Packet packet, ServerConnection connection) {
        checkNotNull(packet, "packet can't be null");
        return connection != null && connection.write(packet);
    }

    @Override
    public boolean transmit(Packet packet, Address target) {
        checkNotNull(packet, "packet can't be null");
        checkNotNull(target, "target can't be null");
        return send(packet, target, null);
    }

    @Override
    public NetworkStats getNetworkStats() {
        return networkStats;
    }

    void refreshNetworkStats() {
        if (networkStats != null) {
            networkStats.refresh();
        }
    }

    private TcpServerConnectionErrorHandler getErrorHandler(Address endpoint, boolean reset) {
        TcpServerConnectionErrorHandler handler = getOrPutIfAbsent(monitors, endpoint, errorHandlerConstructor);
        if (reset) {
            handler.reset();
        }
        return handler;
    }

    Channel newChannel(SocketChannel socketChannel, boolean clientMode) throws IOException {
        Networking networking = server.getNetworking();
        ChannelInitializer channelInitializer = channelInitializerFn.apply(endpointQualifier);
        assert channelInitializer != null : "Found NULL channel initializer for endpoint-qualifier " + endpointQualifier;
        Channel channel = networking.register(channelInitializer, socketChannel, clientMode);
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
        serverContext.onFailedConnection(address);
        if (!silent) {
            getErrorHandler(address, false).onError(t);
        }
    }

    synchronized TcpServerConnection newConnection(Channel channel, Address remoteAddress) {
        try {
            if (!server.isLive()) {
                throw new IllegalStateException("connection manager is not live!");
            }

            TcpServerConnection connection = new TcpServerConnection(this, connectionLifecycleListener,
                    connectionIdGen.incrementAndGet(), channel);

            connection.setRemoteAddress(remoteAddress);
            connections.add(connection);

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
        Connection connection = get(target);
        if (connection != null) {
            return connection.write(packet);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target);
        }

        int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && serverContext.isNodeActive()) {
            getOrConnect(target, true);
            try {
                server.scheduleDeferred(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                return true;
            } catch (RejectedExecutionException e) {
                if (server.isLive()) {
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
        return "TcpServerConnectionManager{"
                + "endpointQualifier=" + endpointQualifier
                + ", mappedConnections=" + mappedConnections + '}';
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor rootDescriptor = descriptor.withPrefix(TCP_PREFIX_CONNECTION);
        if (endpointQualifier == null) {
            context.collect(rootDescriptor.copy(), this);
        } else {
            context.collect(rootDescriptor
                    .copy()
                    .withDiscriminator(TCP_DISCRIMINATOR_ENDPOINT, endpointQualifier.toMetricsPrefixString()), this);
        }

        for (TcpServerConnection connection : connections) {
            if (connection.getRemoteAddress() != null) {
                context.collect(rootDescriptor
                        .copy()
                        .withDiscriminator(TCP_DISCRIMINATOR_ENDPOINT, connection.getRemoteAddress().toString()), connection);
            }
        }

        int clientCount = 0;
        int textCount = 0;
        for (Map.Entry<Address, TcpServerConnection> entry : mappedConnections.entrySet()) {
            Address bindAddress = entry.getKey();
            TcpServerConnection connection = entry.getValue();
            if (connection.isClient()) {
                clientCount++;
                String connectionType = connection.getConnectionType();
                if (REST_CLIENT.equals(connectionType) || MEMCACHE_CLIENT.equals(connectionType)) {
                    textCount++;
                }
            }

            if (connection.getRemoteAddress() != null) {
                context.collect(rootDescriptor
                        .copy()
                        .withDiscriminator(TCP_DISCRIMINATOR_BINDADDRESS, bindAddress.toString())
                        .withTag(TCP_TAG_ENDPOINT, connection.getRemoteAddress().toString()), connection);
            }
        }

        if (endpointConfig == null) {
            context.collect(rootDescriptor.copy(), TCP_METRIC_CLIENT_COUNT, MANDATORY, COUNT, clientCount);
            context.collect(rootDescriptor.copy(), TCP_METRIC_TEXT_COUNT, MANDATORY, COUNT, textCount);
        }
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

    public final class ConnectionLifecycleListenerImpl implements ConnectionLifecycleListener<TcpServerConnection> {

        @Override
        public void onConnectionClose(TcpServerConnection connection, Throwable cause, boolean silent) {
            closedCount.inc();

            connections.remove(connection);

            if (networkStats != null) {
                // Note: this call must happen after activeConnections.remove
                networkStats.onConnectionClose(connection);
            }

            Address remoteAddress = connection.getRemoteAddress();
            if (remoteAddress != null) {
                connectionsInProgress.remove(remoteAddress);
                mappedConnections.remove(remoteAddress, connection);
                fireConnectionRemovedEvent(connection, remoteAddress);
            }

            if (cause != null) {
                serverContext.onFailedConnection(remoteAddress);
                if (!silent) {
                    getErrorHandler(remoteAddress, false).onError(cause);
                }
            }
        }
    }

    private class NetworkStatsImpl implements NetworkStats {
        private final AtomicLong bytesReceivedLastCalc = new AtomicLong();
        private final MwCounter bytesReceivedOnClosed = newMwCounter();
        private final AtomicLong bytesSentLastCalc = new AtomicLong();
        private final MwCounter bytesSentOnClosed = newMwCounter();

        @Override
        public long getBytesReceived() {
            return bytesReceivedLastCalc.get();
        }

        @Override
        public long getBytesSent() {
            return bytesSentLastCalc.get();
        }

        void refresh() {
            MutableLong totalReceived = MutableLong.valueOf(bytesReceivedOnClosed.get());
            MutableLong totalSent = MutableLong.valueOf(bytesSentOnClosed.get());
            connections.forEach(conn -> {
                totalReceived.value += conn.getChannel().bytesRead();
                totalSent.value += conn.getChannel().bytesWritten();
            });
            // counters must be monotonically increasing
            bytesReceivedLastCalc.updateAndGet((v) -> Math.max(v, totalReceived.value));
            bytesSentLastCalc.updateAndGet((v) -> Math.max(v, totalSent.value));
        }

        void onConnectionClose(TcpServerConnection connection) {
            bytesReceivedOnClosed.inc(connection.getChannel().bytesRead());
            bytesSentOnClosed.inc(connection.getChannel().bytesWritten());
        }
    }
}

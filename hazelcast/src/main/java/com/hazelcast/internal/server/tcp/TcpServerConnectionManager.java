/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.executor.StripedRunnable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_BINDADDRESS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_ENDPOINT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_CLIENT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_IN_PROGRESS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_TEXT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_TAG_ENDPOINT;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.nio.ConnectionType.MEMCACHE_CLIENT;
import static com.hazelcast.internal.nio.ConnectionType.REST_CLIENT;
import static com.hazelcast.internal.nio.IOUtil.close;
import static com.hazelcast.internal.nio.IOUtil.setChannelOptions;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableSet;

/**
 * This is the public APIs for connection manager
 * Private APIs and supporting stuff is in the Base class
 */
public class TcpServerConnectionManager extends TcpServerConnectionManagerBase
        implements Consumer<Packet>, DynamicMetricsProvider {
    private final Function<EndpointQualifier, ChannelInitializer> channelInitializerFn;
    private final TcpServerConnector connector;
    private final TcpServerControl serverControl;
    private final AtomicInteger connectionIdGen = new AtomicInteger();

    TcpServerConnectionManager(
            TcpServer server,
            EndpointConfig endpointConfig,
            LocalAddressRegistry addressRegistry,
            Function<EndpointQualifier, ChannelInitializer> channelInitializerFn,
            ServerContext serverContext,
            Set<ProtocolType> supportedProtocolTypes
    ) {
        super(server, endpointConfig, addressRegistry);
        this.channelInitializerFn = channelInitializerFn;
        this.connector = new TcpServerConnector(this);
        this.serverControl = new TcpServerControl(this, serverContext, logger, supportedProtocolTypes);
    }

    @Override
    public TcpServer getServer() {
        return server;
    }

    @Override
    @Nonnull
    public Collection<ServerConnection> getConnections() {
        return unmodifiableSet(connections);
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        checkNotNull(listener, "listener can't be null");
        connectionListeners.add(listener);
    }

    @Override
    public synchronized void accept(Packet packet) {
        serverControl.process(packet);
    }

    @Override
    public ServerConnection get(@Nonnull Address address, int streamId) {
        UUID uuid = addressRegistry.uuidOf(address);
        return uuid != null ? getPlane(streamId).getConnection(uuid) : null;
    }

    @Override
    @Nonnull
    public List<ServerConnection> getAllConnections(@Nonnull Address address) {
        UUID instanceUuid = addressRegistry.uuidOf(address);
        // Because duplicate connections can be established on the planes and
        // we don't keep all duplicates on the planes, we need to iterate over
        // connections set which stores all active connections.
        return instanceUuid != null
                ? connections.stream().filter(connection -> Objects.equals(instanceUuid, connection.getRemoteUuid()))
                                      .collect(Collectors.toList())
                : Collections.emptyList();
    }

    @Override
    public ServerConnection getOrConnect(@Nonnull Address address, int streamId) {
        return getOrConnect(address, false, streamId);
    }

    @Override
    public ServerConnection getOrConnect(@Nonnull final Address address, final boolean silent, int streamId) {
        Plane plane = getPlane(streamId);
        TcpServerConnection connection = null;
        UUID uuid = addressRegistry.uuidOf(address);
        if (uuid != null) {
            connection = plane.getConnection(uuid);
        }
        if (connection == null && server.isLive()) {
            final AtomicBoolean isNotYetInProgress = new AtomicBoolean();
            plane.addConnectionInProgressIfAbsent(address, ignored -> {
                isNotYetInProgress.set(true);
                return connector.asyncConnect(address, silent, plane.index);
            });
            if (isNotYetInProgress.get()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Connection to: " + address + " streamId:" + streamId + " is not yet in progress");
                }
            } else {
                if (logger.isFineEnabled()) {
                    logger.fine("Connection to: " + address + " streamId:" + streamId + " is already in progress");
                }
            }
        }
        return connection;
    }

    @Override
    public synchronized boolean register(
            Address primaryAddress,
            Address targetAddress,
            Collection<Address> remoteAddressAliases,
            UUID remoteUuid,
            final ServerConnection c,
            int planeIndex
    ) {
        Plane plane = planes[planeIndex];
        TcpServerConnection connection = (TcpServerConnection) c;
        try {
            if (!connection.isAlive()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(connection + " to " + remoteUuid + " is not registered since connection is not active.");
                }
                return false;
            }

            connection.setRemoteAddress(primaryAddress);
            connection.setRemoteUuid(remoteUuid);

            if (!connection.isClient()) {
                connection.setErrorHandler(getErrorHandler(primaryAddress, plane.index).reset());
            }

            registerAddresses(remoteUuid, primaryAddress, targetAddress, remoteAddressAliases);

            // handle error cases after registering the addresses to avoid the later failing connections
            // occur because target addresses are not registered.

            // handle self connection
            if (remoteUuid.equals(serverContext.getThisUuid())) {
                connection.close("Connecting to self!", null);
                return false;
            }

            plane.putConnection(remoteUuid, connection);

            serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    connectionListeners.forEach(listener -> listener.connectionAdded(connection));
                }

                @Override
                public int getKey() {
                    return primaryAddress.hashCode();
                }
            });

            return true;
        } finally {
            if (targetAddress != null) {
                plane.removeConnectionInProgress(targetAddress);
            }
        }
    }

    public synchronized void reset(boolean cleanListeners) {
        acceptedChannels.forEach(IOUtil::closeResource);
        for (Plane plane : planes) {
            plane.forEachConnection(conn -> close(conn, "TcpServer is stopping"));
            plane.clearConnections();
        }

        connections.forEach(conn -> close(conn, "TcpServer is stopping"));
        acceptedChannels.clear();
        stream(planes).forEach(plane -> plane.clearConnectionsInProgress());
        stream(planes).forEach(plane -> plane.errorHandlers.clear());

        connections.clear();
        addressRegistry.reset();

        if (cleanListeners) {
            connectionListeners.clear();
        }
    }

    @Override
    public boolean transmit(Packet packet, Address targetAddress, int streamId) {
        checkNotNull(packet, "packet can't be null");
        checkNotNull(targetAddress, "targetAddress can't be null");
        return send(packet, targetAddress, null, streamId);
    }

    @Override
    public NetworkStats getNetworkStats() {
        return networkStats;
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

    void failedConnection(Address address, int planeIndex, Throwable t, boolean silent) {
        planes[planeIndex].removeConnectionInProgress(address);
        serverContext.onFailedConnection(address);
        if (!silent) {
            getErrorHandler(address, planeIndex).onError(t);
        }
    }

    synchronized TcpServerConnection newConnection(Channel channel, Address remoteAddress, boolean acceptorSide) {
        try {
            if (!server.isLive()) {
                throw new IllegalStateException("connection manager is not live!");
            }

            TcpServerConnection connection = new TcpServerConnection(this, connectionLifecycleListener,
                    connectionIdGen.incrementAndGet(), channel, acceptorSide);

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

    @Override
    public String toString() {
        return "TcpServerConnectionManager{"
                + "endpointQualifier=" + endpointQualifier
                + ", connectionsMap=" + null + '}';
    }

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_IN_PROGRESS_COUNT, level = DEBUG)
    private int connectionsInProgress() {
        int c = 0;
        for (Plane plane : planes) {
            c += plane.connectionsInProgressCount();
        }
        return c;
    }

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_COUNT, level = MANDATORY)
    public int connectionCount() {
        int c = 0;
        for (Plane plane : planes) {
            c += plane.connectionCount();
        }
        return c;
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
        for (Plane plane : planes) {
            for (Map.Entry<UUID, TcpServerConnection> entry : plane.connections()) {
                UUID uuid = entry.getKey();
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
                            .withDiscriminator(TCP_DISCRIMINATOR_BINDADDRESS, uuid.toString())
                            .withTag(TCP_TAG_ENDPOINT, connection.getRemoteAddress().toString()), connection);
                }
            }
        }

        if (endpointConfig == null) {
            context.collect(rootDescriptor.copy(), TCP_METRIC_CLIENT_COUNT, MANDATORY, COUNT, clientCount);
            context.collect(rootDescriptor.copy(), TCP_METRIC_TEXT_COUNT, MANDATORY, COUNT, textCount);
        }
    }

    @Override
    public boolean blockOnConnect(Address address, long timeoutMillis, int streamId) throws InterruptedException {
        Plane plane = getPlane(streamId);
        try {
            Future<Void> future = plane.getConnectionInProgress(address);
            if (future != null) {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                return future.isDone();
            } else {
                // connection-in-progress has come and gone,
                // so we are not timed out here
                return true;
            }
        } catch (ExecutionException | TimeoutException ex) {
            throw ExceptionUtil.rethrow(ex);
        }
    }
}

/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.server.tcp.LinkedAddresses.getAllLinkedAddresses;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.spi.properties.ClusterProperty.CHANNEL_COUNT;
import static java.util.Collections.newSetFromMap;

/**
 * This base class solely exists for the purpose of simplification of the Manager class
 * This contains the private functionality required by the Manager
 */
abstract class TcpServerConnectionManagerBase implements ServerConnectionManager {
    private static final int RETRY_NUMBER = 5;
    private static final long DELAY_FACTOR = 100L;

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT)
    protected final MwCounter openedCount = newMwCounter();

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT)
    protected final MwCounter closedCount = newMwCounter();

    protected final ILogger logger;
    protected final TcpServer server;
    protected final ServerContext serverContext;
    protected final NetworkStatsImpl networkStats;
    protected final EndpointConfig endpointConfig;
    protected final EndpointQualifier endpointQualifier;
    protected final LocalAddressRegistry addressRegistry;

    final Plane[] planes;
    final int planeCount;

    final ConnectionLifecycleListenerImpl connectionLifecycleListener = new ConnectionLifecycleListenerImpl();
    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT, level = MANDATORY)
    final Set<TcpServerConnection> connections = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT, level = MANDATORY)
    final Set<Channel> acceptedChannels = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT)
    final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<>();

    private final ConstructorFunction<Address, TcpServerConnectionErrorHandler> errorHandlerConstructor;

    TcpServerConnectionManagerBase(TcpServer tcpServer, EndpointConfig endpointConfig, LocalAddressRegistry addressRegistry) {
        this.server = tcpServer;
        this.errorHandlerConstructor = endpoint -> new TcpServerConnectionErrorHandler(
                tcpServer.getContext(),
                endpoint
        );
        this.serverContext = tcpServer.getContext();
        this.endpointConfig = endpointConfig;
        this.endpointQualifier = endpointConfig != null ? endpointConfig.getQualifier() : null;
        this.addressRegistry = addressRegistry;
        this.logger = serverContext.getLoggingService().getLogger(TcpServerConnectionManager.class);
        this.networkStats = endpointQualifier == null ? null : new NetworkStatsImpl();
        this.planeCount = serverContext.properties().getInteger(CHANNEL_COUNT);
        this.planes = new Plane[planeCount];
        for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
            planes[planeIndex] = new Plane(planeIndex);
        }
    }

    public EndpointQualifier getEndpointQualifier() {
        return endpointQualifier;
    }

    void refreshNetworkStats() {
        if (networkStats != null) {
            networkStats.refresh();
        }
    }

    static class Plane {
        final ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> errorHandlers = new ConcurrentHashMap<>(100);
        final int index;

        private final Map<LinkedAddresses, Future<Void>> connectionsInProgress = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<UUID, TcpServerConnection> connectionMap = new ConcurrentHashMap<>(100);

        Plane(int index) {
            this.index = index;
        }

        TcpServerConnection getConnection(UUID uuid) {
            return connectionMap.get(uuid);
        }

        void putConnection(UUID uuid, TcpServerConnection connection) {
            connectionMap.put(uuid, connection);
        }

        void putConnectionIfAbsent(UUID uuid, TcpServerConnection connection) {
            connectionMap.putIfAbsent(uuid, connection);
        }

        void removeConnection(TcpServerConnection connection) {
            removeConnectionInProgress(connection.getRemoteAddress());

            // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
            Iterator<TcpServerConnection> connections = connectionMap.values().iterator();
            while (connections.hasNext()) {
                TcpServerConnection c = connections.next();
                if (c.equals(connection)) {
                    connections.remove();
                }
            }
        }

        public boolean removeConnectionsWithId(int id) {
            // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
            boolean found = false;
            Iterator<TcpServerConnection> connections = connectionMap.values().iterator();
            while (connections.hasNext()) {
                TcpServerConnection c = connections.next();
                if (c.getConnectionId() == id) {
                    connections.remove();
                    found = true;
                }
            }
            return found;
        }

        public void forEachConnection(Consumer<? super TcpServerConnection> consumer) {
            connectionMap.values().forEach(consumer);
        }

        public void clearConnections() {
            connectionMap.clear();
        }

        public Set<Map.Entry<UUID, TcpServerConnection>> connections() {
            return Collections.unmodifiableSet(connectionMap.entrySet());
        }

        public int connectionCount() {
            return (int) connectionMap.values().stream().distinct().count();
        }

        public boolean hasConnectionInProgress(Address address) {
            return connectionsInProgress.keySet()
                    .stream()
                    .anyMatch(linkedAddresses -> linkedAddresses.contains(address));
        }

        public Address getConnectedAddress(Address address) {
            for (LinkedAddresses addresses : connectionsInProgress.keySet()) {
                if (addresses.contains(address)) {
                    return addresses.getPrimaryAddress();
                }
            }
            // not found in in-progress connections
            return address;
        }

        public Future<Void> getConnectionInProgress(Address address) {
            for (Map.Entry<LinkedAddresses, Future<Void>> entry : connectionsInProgress.entrySet()) {
                if (entry.getKey().contains(address)) {
                    return entry.getValue();
                }
            }
            return null;
        }

        public void addConnectionInProgressIfAbsent(
                Address address,
                Function<? super LinkedAddresses, ? extends Future<Void>> mappingFn
        ) {
            connectionsInProgress.computeIfAbsent(getAllLinkedAddresses(address), mappingFn);
        }

        public boolean removeConnectionInProgress(Address address) {
            // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
            boolean found = false;
            Iterator<LinkedAddresses> linkedAddressesIterator = connectionsInProgress.keySet().iterator();
            while (linkedAddressesIterator.hasNext()) {
                LinkedAddresses addresses = linkedAddressesIterator.next();
                if (addresses.contains(address)) {
                    linkedAddressesIterator.remove();
                    found = true;
                }
            }
            return found;
        }

        public void clearConnectionsInProgress() {
            connectionsInProgress.clear();
        }

        public int connectionsInProgressCount() {
            return connectionsInProgress.size();
        }
    }

    private final class SendTask implements Runnable {
        private final Packet packet;
        private final Address targetAddress;
        private final int streamId;
        private volatile int retries;

        private SendTask(Packet packet, Address targetAddress, int streamId) {
            this.packet = packet;
            this.targetAddress = targetAddress;
            this.streamId = streamId;
        }

        @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "single-writer, many-reader")
        @Override
        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + targetAddress);
            }
            send(packet, targetAddress, this, streamId);
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

    private final class ConnectionLifecycleListenerImpl implements ConnectionLifecycleListener<TcpServerConnection> {
        @Override
        public void onConnectionClose(TcpServerConnection connection, Throwable cause, boolean silent) {
            closedCount.inc();

            connections.remove(connection);

            if (networkStats != null) {
                // Note: this call must happen after activeConnections.remove
                networkStats.onConnectionClose(connection);
            }

            Address remoteAddress = connection.getRemoteAddress();
            if (remoteAddress == null) {
                return;
            }

            Plane plane = null;
            int planeIndex = connection.getPlaneIndex();
            if (planeIndex > -1) {
                plane = planes[connection.getPlaneIndex()];
                plane.removeConnection(connection);
                fireConnectionRemovedEvent(connection, remoteAddress);
            } else {
                // it might be the case that the connection was closed quickly enough
                // that the planeIndex was not set. Instead look for the connection
                // by remoteAddress and connectionId over all planes and remove it wherever found.
                boolean removed = false;
                for (Plane p : planes) {
                    if (p.removeConnectionInProgress(remoteAddress)) {
                        plane = p;
                    }
                    if (p.removeConnectionsWithId(connection.getConnectionId())) {
                        plane = p;
                        removed = true;
                    }
                }
                if (removed) {
                    fireConnectionRemovedEvent(connection, remoteAddress);
                }
            }

            long lastReadTime = connection.getChannel().lastReadTimeMillis();
            boolean hadNoDataTraffic = lastReadTime < 0;
            if (hadNoDataTraffic) {
                serverContext.onFailedConnection(remoteAddress);
                if (!silent && plane != null) {
                    getErrorHandler(remoteAddress, plane.errorHandlers).onError(cause);
                }
            }
        }
    }

    protected boolean send(Packet packet, Address target, SendTask sendTask, int streamId) {
        UUID targetUuid = addressRegistry.uuidOf(target);
        Connection connection = get(targetUuid, streamId);
        if (connection != null) {
            return connection.write(packet);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target, streamId);
        }

        int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && serverContext.isNodeActive()) {
            getOrConnect(target, true, streamId);
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

    protected TcpServerConnectionErrorHandler getErrorHandler(Address endpoint, int planeIndex) {
        ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> errorHandlers = planes[planeIndex].errorHandlers;
        return getErrorHandler(endpoint, errorHandlers);
    }

    private TcpServerConnectionErrorHandler getErrorHandler(
            Address endpoint,
            ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> errorHandlers
    ) {
        return getOrPutIfAbsent(errorHandlers, endpoint, errorHandlerConstructor);
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

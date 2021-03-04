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
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT;
import com.hazelcast.internal.metrics.Probe;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.counters.MwCounter;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import static com.hazelcast.spi.properties.ClusterProperty.CHANNEL_COUNT;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import static java.util.Collections.newSetFromMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    TcpServerConnectionManagerBase(TcpServer tcpServer, EndpointConfig endpointConfig) {
        this.server = tcpServer;
        this.errorHandlerConstructor = endpoint -> new TcpServerConnectionErrorHandler(tcpServer.getContext(), endpoint);
        this.serverContext = tcpServer.getContext();
        this.endpointConfig = endpointConfig;
        this.endpointQualifier = endpointConfig != null ? endpointConfig.getQualifier() : null;
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
        final ConcurrentHashMap<Address, TcpServerConnection> connectionMap = new ConcurrentHashMap<>(100);
        final Set<Address> connectionsInProgress = newSetFromMap(new ConcurrentHashMap<>());
        final ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> errorHandlers = new ConcurrentHashMap<>(100);
        final int index;

        Plane(int index) {
            this.index = index;
        }
    }

    private final class SendTask implements Runnable {
        private final Packet packet;
        private final Address target;
        private final int streamId;
        private volatile int retries;

        private SendTask(Packet packet, Address target, int streamId) {
            this.packet = packet;
            this.target = target;
            this.streamId = streamId;
        }

        @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "single-writer, many-reader")
        @Override
        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this, streamId);
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
            if (remoteAddress != null) {
                int planeIndex = connection.getPlaneIndex();
                if (planeIndex > -1) {
                    Plane plane = planes[connection.getPlaneIndex()];
                    plane.connectionsInProgress.remove(remoteAddress);
                    plane.connectionMap.remove(remoteAddress);
                    fireConnectionRemovedEvent(connection, remoteAddress);
                } else {
                    // it might be the case that the connection was closed quickly enough
                    // that the planeIndex was not set. Instead look for the connection
                    // by remoteAddress and connectionId over all planes and remove it wherever found.
                    boolean removed = false;
                    for (Plane plane : planes) {
                        plane.connectionsInProgress.remove(remoteAddress);
                        // not using removeIf due to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8078645
                        Iterator<TcpServerConnection> connections = plane.connectionMap.values().iterator();
                        while (connections.hasNext()) {
                            TcpServerConnection cxn = connections.next();
                            if (cxn.getConnectionId() == connection.getConnectionId()) {
                                connections.remove();
                                removed = true;
                            }
                        }
                    }
                    if (removed) {
                        fireConnectionRemovedEvent(connection, remoteAddress);
                    }
                }
            }

            if (cause != null) {
                serverContext.onFailedConnection(remoteAddress);
                if (!silent) {
                    getErrorHandler(remoteAddress, connection.getPlaneIndex(), false).onError(cause);
                }
            }
        }
    }

    protected boolean send(Packet packet, Address target, SendTask sendTask, int streamId) {
        Connection connection = get(target, streamId);
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

    protected TcpServerConnectionErrorHandler getErrorHandler(Address endpoint, int planeIndex, boolean reset) {
        ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> errorHandlers = planes[planeIndex].errorHandlers;
        TcpServerConnectionErrorHandler handler = getOrPutIfAbsent(errorHandlers, endpoint, errorHandlerConstructor);
        if (reset) {
            handler.reset();
        }
        return handler;
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

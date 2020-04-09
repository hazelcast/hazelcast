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

package com.hazelcast.internal.server;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.NetworkStats;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link ServerConnectionManager} wrapper which adds firewalling capabilities.
 * All methods delegate to the original ConnectionManager.
 */
public class FirewallingServer
        implements Server, Consumer<Packet> {

    private final ScheduledExecutorService scheduledExecutor
            = newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FirewallingConnectionManager"));
    private final Set<Address> blockedAddresses = newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final Server delegate;
    private final Consumer<Packet> packetConsumer;
    private AtomicReference<ServerConnectionManager> endpointManagerRef = new AtomicReference<ServerConnectionManager>(null);

    @SuppressWarnings("unchecked")
    public FirewallingServer(Server delegate, Set<Address> initiallyBlockedAddresses) {
        this.delegate = delegate;
        this.blockedAddresses.addAll(initiallyBlockedAddresses);
        this.packetConsumer = delegate.getConnectionManager(MEMBER);
    }

    @Override
    public IOService getIoService() {
        return delegate.getIoService();
    }

    @Override
    public AggregateServerConnectionManager getAggregateConnectionManager() {
        final AggregateServerConnectionManager delegateAggregateEndpointManager = this.delegate.getAggregateConnectionManager();
        return new FirewallingAggregateConnectionManager(delegateAggregateEndpointManager);
    }

    @Override
    public boolean isLive() {
        return delegate.isLive();
    }

    @Override
    public ServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
        if (endpointManagerRef.get() == null) {
            final ServerConnectionManager delegateServerConnectionManager = this.delegate.getConnectionManager(MEMBER);
            endpointManagerRef.compareAndSet(null, new FirewallingEndpointManager(delegateServerConnectionManager));
        }

        //TODO (TK) : Hint this manager was also delegating getClientConnections etc. I removed them, are they needed (verify during test)
        return endpointManagerRef.get();
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
        scheduledExecutor.shutdown();
    }

    @Override
    public void accept(Packet packet) {
        if (packetConsumer == null) {
            throw new UnsupportedOperationException(delegate + " is not instance of Consumer!");
        }
        packetConsumer.accept(packet);
    }

    private class DelayedPacketTask implements Runnable {

        Packet packet;
        ServerConnection connection;
        Address target;

        DelayedPacketTask(Packet packet, ServerConnection connection) {
            this.packet = packet;
            this.connection = checkNotNull(connection);
        }

        DelayedPacketTask(Packet packet, Address target) {
            this.packet = packet;
            this.target = checkNotNull(target);
        }

        @Override
        public void run() {
            if (connection != null) {
                delegate.getConnectionManager(MEMBER).transmit(packet, connection);
            } else {
                delegate.getConnectionManager(MEMBER).transmit(packet, target);
            }
        }
    }

    private static class PacketDelayProps {

        final long minDelayMs;
        final long maxDelayMs;

        private PacketDelayProps(long minDelayMs, long maxDelayMs) {
            checkPositive(minDelayMs, "minDelayMs must be positive, but was " + minDelayMs);
            checkPositive(maxDelayMs, "maxDelayMs must be positive, but was " + maxDelayMs);
            checkState(maxDelayMs >= minDelayMs, "maxDelayMs must not be smaller than minDelayMs (maxDelayMs: " + maxDelayMs
                    + ", minDelayMs: " + minDelayMs + ")");

            this.minDelayMs = minDelayMs;
            this.maxDelayMs = maxDelayMs;
        }
    }

    public class FirewallingAggregateConnectionManager
            implements AggregateServerConnectionManager {

        final AggregateServerConnectionManager delegate;

        FirewallingAggregateConnectionManager(AggregateServerConnectionManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public Collection getActiveConnections() {
            return delegate.getActiveConnections();
        }

        @Override
        public Collection getConnections() {
            return delegate.getConnections();
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
            delegate.addConnectionListener(listener);
        }

        @Override
        public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
            return delegate.getNetworkStats();
        }
    }

    public class FirewallingEndpointManager
            implements ServerConnectionManager {

        private volatile PacketFilter   packetFilter;
        private volatile PacketDelayProps delayProps = new PacketDelayProps(500, 5000);

        final ServerConnectionManager delegate;

        FirewallingEndpointManager(ServerConnectionManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public Server getServer() {
            return delegate.getServer();
        }

        @Override
        public void accept(Packet packet) {
            delegate.accept(packet);
        }

        @Override
        public synchronized ServerConnection getOrConnect(Address address, boolean silent) {
            return getOrConnect(address);
        }

        public synchronized void blockNewConnection(Address address) {
            blockedAddresses.add(address);
        }

        public synchronized void closeActiveConnection(Address address) {
            ServerConnection connection = get(address);
            if (connection != null) {
                connection.close("Blocked by connection manager", null);
            }
        }

        public synchronized void unblock(Address address) {
            blockedAddresses.remove(address);
            Connection connection = get(address);
            if (connection instanceof DroppingServerConnection) {
                connection.close(null, null);
            }
        }

        public void setPacketFilter(PacketFilter packetFilter) {
            this.packetFilter = checkNotNull(packetFilter, "the packetFilter cannot be null");
        }

        public void removePacketFilter() {
            packetFilter = null;
        }

        public void setDelayMillis(long minDelayMs, long maxDelayMs) {
            delayProps = new PacketDelayProps(minDelayMs, maxDelayMs);
        }

        private PacketFilter.Action applyFilter(Packet packet, Address target) {
            if (blockedAddresses.contains(target)) {
                return PacketFilter.Action.REJECT;
            }

            PacketFilter filter = packetFilter;
            return filter == null ? PacketFilter.Action.ALLOW : filter.filter(packet, target);
        }

        private long getDelayMs() {
            PacketDelayProps delay = delayProps;
            return getRandomBetween(delay.maxDelayMs, delay.minDelayMs);
        }

        private long getRandomBetween(long max, long min) {
            return (long) ((max - min) * Math.random() + min);
        }

        @Override
        public boolean transmit(Packet packet, ServerConnection connection) {
            if (connection != null) {
                PacketFilter.Action action = applyFilter(packet, connection.getRemoteAddress());
                switch (action) {
                    case DROP:
                        return true;
                    case REJECT:
                        return false;
                    case DELAY:
                        scheduledExecutor.schedule(new DelayedPacketTask(packet, connection), getDelayMs(), MILLISECONDS);
                        return true;
                    default:
                        // NOP
                }
            }
            return delegate.transmit(packet, connection);
        }

        @Override
        public boolean transmit(Packet packet, Address target) {
            PacketFilter.Action action = applyFilter(packet, target);
            switch (action) {
                case DROP:
                    return true;
                case REJECT:
                    return false;
                case DELAY:
                    scheduledExecutor.schedule(new DelayedPacketTask(packet, target), getDelayMs(), MILLISECONDS);
                    return true;
                default:
                    return delegate.transmit(packet, target);
            }
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
            delegate.addConnectionListener(listener);
        }

        @Override
        public Collection<ServerConnection> getConnections() {
            return delegate.getConnections();
        }

        @Override
        public Collection<ServerConnection> getActiveConnections() {
            return delegate.getActiveConnections();
        }

        @Override
        public ServerConnection get(Address address) {
            return delegate.get(address);
        }

        @Override
        public ServerConnection getOrConnect(Address address) {
            return delegate.getOrConnect(address);
        }

        @Override
        public boolean register(Address remoteAddress, ServerConnection connection) {
            return delegate.register(remoteAddress, connection);
        }

        @Override
        public NetworkStats getNetworkStats() {
            return delegate.getNetworkStats();
        }
    }
}

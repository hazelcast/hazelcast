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

package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;

import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    public final Server delegate;
    private final ScheduledExecutorService scheduledExecutor
            = newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FirewallingConnectionManager"));
    private final Set<Address> blockedAddresses = newSetFromMap(new ConcurrentHashMap<>());

    private final Consumer<Packet> packetConsumer;
    private final AtomicReference<ServerConnectionManager> connectionManagerRef = new AtomicReference<>(null);

    @SuppressWarnings("unchecked")
    public FirewallingServer(Server delegate, Set<Address> initiallyBlockedAddresses) {
        this.delegate = delegate;
        this.blockedAddresses.addAll(initiallyBlockedAddresses);
        this.packetConsumer = delegate.getConnectionManager(MEMBER);
    }

    @Override
    public ServerContext getContext() {
        return delegate.getContext();
    }

    @Override
    public @Nonnull
    Collection getConnections() {
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

    @Override
    public boolean isLive() {
        return delegate.isLive();
    }

    @Override
    public ServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
        if (connectionManagerRef.get() == null) {
            final ServerConnectionManager delegateServerConnectionManager = this.delegate.getConnectionManager(MEMBER);
            connectionManagerRef.compareAndSet(null, new FirewallingServerConnectionManager(delegateServerConnectionManager));
        }

        //TODO (TK) : Hint this manager was also delegating getClientConnections etc. I removed them, are they needed (verify during test)
        return connectionManagerRef.get();
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
                connection.write(packet);
            } else {
                delegate.getConnectionManager(MEMBER).transmit(packet, target);
            }
        }
    }

    private static class PacketDelayProps {

        final long minDelayMs;
        final long maxDelayMs;

        private PacketDelayProps(long minDelayMs, long maxDelayMs) {
            checkPositive("minDelayMs", minDelayMs);
            checkPositive("maxDelayMs", maxDelayMs);
            checkState(maxDelayMs >= minDelayMs, "maxDelayMs must not be smaller than minDelayMs (maxDelayMs: " + maxDelayMs
                    + ", minDelayMs: " + minDelayMs + ")");

            this.minDelayMs = minDelayMs;
            this.maxDelayMs = maxDelayMs;
        }
    }

    public class FirewallingServerConnectionManager
            implements ServerConnectionManager {

        private volatile PacketFilter packetFilter;
        private volatile PacketDelayProps delayProps = new PacketDelayProps(500, 5000);

        final ServerConnectionManager delegate;

        FirewallingServerConnectionManager(ServerConnectionManager delegate) {
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
        public boolean transmit(Packet packet, Address target, int streamId) {
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
                    return delegate.transmit(packet, target, streamId);
            }
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
            delegate.addConnectionListener(listener);
        }

        @Override
        public @Nonnull
        Collection<ServerConnection> getConnections() {
            return delegate.getConnections();
        }

        @Override
        public int connectionCount() {
            return delegate.connectionCount();
        }

        @Override
        public ServerConnection get(@Nonnull Address address, int streamId) {
            return wrap(delegate.get(address, streamId));
        }

        @Override
        @Nonnull
        public List<ServerConnection> getAllConnections(@Nonnull Address address) {
            return delegate.getAllConnections(address).stream().map(this::wrap).collect(Collectors.toList());
        }

        private ServerConnection wrap(ServerConnection c) {
            return c == null ? null : new FirewallingConnection(c, this);
        }

        @Override
        public synchronized ServerConnection getOrConnect(@Nonnull Address address, boolean silent, int streamId) {
            return wrap(delegate.getOrConnect(address, streamId));
        }

        @Override
        public synchronized ServerConnection getOrConnect(@Nonnull Address address, int streamId) {
            return wrap(delegate.getOrConnect(address, streamId));
        }

        @Override
        public boolean blockOnConnect(Address address,
                                      long timeoutMillis, int streamId) throws InterruptedException {
            return delegate.blockOnConnect(address, timeoutMillis, streamId);
        }

        @Override
        public boolean register(
                Address remoteAddress,
                Address targetAddress,
                Collection<Address> remoteAddressAliases,
                UUID remoteUuid,
                ServerConnection connection,
                int streamId
        ) {
            return delegate.register(remoteAddress, targetAddress, remoteAddressAliases, remoteUuid, connection, streamId);
        }

        @Override
        public NetworkStats getNetworkStats() {
            return delegate.getNetworkStats();
        }
    }

    private class FirewallingConnection implements ServerConnection {
        private ServerConnection delegate;
        private FirewallingServerConnectionManager firewallingServerConnectionManager;

        FirewallingConnection(ServerConnection delegate, FirewallingServerConnectionManager firewallingServerConnectionManager) {
            this.delegate = delegate;
            this.firewallingServerConnectionManager = firewallingServerConnectionManager;
        }

        @Override
        public ServerConnectionManager getConnectionManager() {
            return firewallingServerConnectionManager;
        }

        @Override
        public String getConnectionType() {
            return delegate.getConnectionType();
        }

        @Override
        public void setConnectionType(String connectionType) {
            delegate.setConnectionType(connectionType);
        }

        @Override
        public boolean isClient() {
            return delegate.isClient();
        }

        @Override
        public ConcurrentMap attributeMap() {
            return delegate.attributeMap();
        }

        @Override
        public boolean isAlive() {
            return delegate.isAlive();
        }

        @Override
        public long lastReadTimeMillis() {
            return delegate.lastReadTimeMillis();
        }

        @Override
        public long lastWriteTimeMillis() {
            return delegate.lastWriteTimeMillis();
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            return delegate.getRemoteSocketAddress();
        }

        @Override
        public Address getRemoteAddress() {
            return delegate.getRemoteAddress();
        }

        @Override
        public void setRemoteAddress(Address remoteAddress) {
            delegate.setRemoteAddress(remoteAddress);
        }

        @Override
        public UUID getRemoteUuid() {
            return delegate.getRemoteUuid();
        }

        @Override
        public void setRemoteUuid(UUID remoteUuid) {
            delegate.setRemoteUuid(remoteUuid);
        }

        @Override
        public InetAddress getInetAddress() {
            return delegate.getInetAddress();
        }

        @Override
        public boolean write(OutboundFrame packet) {
            PacketFilter.Action action = firewallingServerConnectionManager.applyFilter((Packet) packet, getRemoteAddress());
            switch (action) {
                case DROP:
                    return true;
                case REJECT:
                    return false;
                case DELAY:
                    scheduledExecutor.schedule(new DelayedPacketTask((Packet) packet, delegate),
                            firewallingServerConnectionManager.getDelayMs(), MILLISECONDS);
                    return true;
                case ALLOW:
                    return delegate.write(packet);
                default:
                    throw new RuntimeException();
            }
        }

        @Override
        public void close(String reason, Throwable cause) {
            delegate.close(reason, cause);
        }

        @Override
        public String getCloseReason() {
            return delegate.getCloseReason();
        }

        @Override
        public Throwable getCloseCause() {
            return delegate.getCloseCause();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FirewallingConnection that = (FirewallingConnection) o;
            return delegate == that.delegate;
        }
    }

}

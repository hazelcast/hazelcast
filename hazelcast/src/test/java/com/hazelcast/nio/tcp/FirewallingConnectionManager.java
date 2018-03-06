/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link ConnectionManager} wrapper which adds firewalling capabilities.
 * All methods delegate to the original ConnectionManager.
 */
public class FirewallingConnectionManager implements ConnectionManager, PacketHandler {

    private final ConnectionManager delegate;
    private final Set<Address> blockedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());
    private final ScheduledExecutorService scheduledExecutor
            = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FirewallingConnectionManager"));
    private final PacketHandler packetHandler;

    private volatile PacketFilter packetFilter;
    private volatile PacketDelayProps delayProps = new PacketDelayProps(500, 5000);

    public FirewallingConnectionManager(ConnectionManager delegate, Set<Address> initiallyBlockedAddresses) {
        this.delegate = delegate;
        this.blockedAddresses.addAll(initiallyBlockedAddresses);
        packetHandler = delegate instanceof PacketHandler ? (PacketHandler) delegate : null;
    }

    @Override
    public synchronized Connection getOrConnect(Address address) {
        Connection connection = getConnection(address);
        if (connection != null && connection.isAlive()) {
            return connection;
        }
        if (blockedAddresses.contains(address)) {
            connection = new DroppingConnection(address, this);
            registerConnection(address, connection);
            return connection;
        } else {
            return delegate.getOrConnect(address);
        }
    }

    @Override
    public synchronized Connection getOrConnect(Address address, boolean silent) {
        return getOrConnect(address);
    }

    public synchronized void blockNewConnection(Address address) {
        blockedAddresses.add(address);
    }

    public synchronized void closeActiveConnection(Address address) {
        Connection connection = getConnection(address);
        if (connection != null) {
            connection.close("Blocked by connection manager", null);
        }
    }

    public synchronized void unblock(Address address) {
        blockedAddresses.remove(address);
        Connection connection = getConnection(address);
        if (connection instanceof DroppingConnection) {
            connection.close(null, null);
        }
    }

    public void setPacketFilter(PacketFilter packetFilter) {
        assert packetFilter != null;
        this.packetFilter = packetFilter;
    }

    public void removePacketFilter() {
        packetFilter = null;
    }

    public void setDelayMillis(long minDelayMs, long maxDelayMs) {
        assert minDelayMs > 0 : "Min delay must be positive: " + minDelayMs;
        assert maxDelayMs > 0 : "Max delay must be positive: " + minDelayMs;
        assert maxDelayMs >= minDelayMs : "Max delay must not be smaller than min delay. Max: "
                + maxDelayMs + ", min: " + minDelayMs;
        this.delayProps = new PacketDelayProps(minDelayMs, maxDelayMs);
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

    private static long getRandomBetween(long max, long min) {
        return (long) ((max - min) * Math.random() + min);
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
        if (connection != null) {
            PacketFilter.Action action = applyFilter(packet, connection.getEndPoint());
            switch (action) {
                case DROP:
                    return true;
                case REJECT:
                    return false;
                case DELAY:
                    scheduledExecutor.schedule(new DelayedPacketTask(packet, connection), getDelayMs(), MILLISECONDS);
                    return true;
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
        }
        return delegate.transmit(packet, target);
    }

    @Override
    public int getCurrentClientConnections() {
        return delegate.getCurrentClientConnections();
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        delegate.addConnectionListener(listener);
    }

    @Override
    public int getAllTextConnections() {
        return delegate.getAllTextConnections();
    }

    @Override
    public int getConnectionCount() {
        return delegate.getConnectionCount();
    }

    @Override
    public int getActiveConnectionCount() {
        return delegate.getActiveConnectionCount();
    }

    @Override
    public Connection getConnection(Address address) {
        return delegate.getConnection(address);
    }

    @Override
    public boolean registerConnection(Address address, Connection connection) {
        return delegate.registerConnection(address, connection);
    }

    @Override
    public void onConnectionClose(Connection connection) {
        delegate.onConnectionClose(connection);
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
    public void handle(Packet packet) throws Exception {
        if (packetHandler == null) {
            throw new UnsupportedOperationException(delegate + " is not instance of PacketHandler!");
        }
        packetHandler.handle(packet);
    }

    private class DelayedPacketTask implements Runnable {
        Packet packet;
        Connection connection;
        Address target;

        DelayedPacketTask(Packet packet, Connection connection) {
            assert connection != null;
            this.packet = packet;
            this.connection = connection;
        }

        DelayedPacketTask(Packet packet, Address target) {
            assert target != null;
            this.packet = packet;
            this.target = target;
        }

        @Override
        public void run() {
            if (connection != null) {
                delegate.transmit(packet, connection);
            } else {
                delegate.transmit(packet, target);
            }
        }
    }

    private static class PacketDelayProps {
        final long minDelayMs;
        final long maxDelayMs;

        private PacketDelayProps(long minDelayMs, long maxDelayMs) {
            this.minDelayMs = minDelayMs;
            this.maxDelayMs = maxDelayMs;
        }
    }
}

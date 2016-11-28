/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.mocknetwork.MockConnectionManager;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FirewallingMockConnectionManager extends MockConnectionManager {

    private final Set<Address> blockedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private volatile PacketFilter droppingPacketFilter;
    private volatile PacketFilter delayingPacketFilter;

    public FirewallingMockConnectionManager(IOService ioService, Node node, TestNodeRegistry registry) {
        super(ioService, node, registry);
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
            return super.getOrConnect(address);
        }
    }

    @Override
    public synchronized Connection getOrConnect(Address address, boolean silent) {
        return getOrConnect(address);
    }

    public synchronized void block(Address address) {
        blockedAddresses.add(address);
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

    public void setDroppingPacketFilter(PacketFilter droppingPacketFilter) {
        this.droppingPacketFilter = droppingPacketFilter;
    }

    public void setDelayingPacketFilter(PacketFilter delayingPacketFilter) {
        this.delayingPacketFilter = delayingPacketFilter;
    }

    private boolean isAllowed(Packet packet, Address target) {
        boolean allowed = true;
        PacketFilter filter = droppingPacketFilter;
        if (filter != null) {
            allowed = filter.allow(packet, target);
        }
        return allowed;
    }

    private boolean isDelayed(Packet packet, Address target) {
        boolean delayed = false;
        PacketFilter filter = delayingPacketFilter;
        if (filter != null) {
            delayed = !filter.allow(packet, target);
        }
        return delayed;
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
        if (connection != null) {
            if (!isAllowed(packet, connection.getEndPoint())) {
                return false;
            }
            if (isDelayed(packet, connection.getEndPoint())) {
                scheduledExecutor.schedule(new DelayedPacketTask(packet, connection), randomDelay(), NANOSECONDS);
                return true;
            }
        }
        return super.transmit(packet, connection);
    }

    @Override
    public boolean transmit(Packet packet, Address target) {
        if (!isAllowed(packet, target)) {
            return false;
        }
        if (isDelayed(packet, target)) {
            scheduledExecutor.schedule(new DelayedPacketTask(packet, target), randomDelay(), NANOSECONDS);
            return true;
        }
        return super.transmit(packet, target);
    }

    private static long randomDelay() {
        return (long) (TimeUnit.SECONDS.toNanos(1) * Math.random());
    }

    @Override
    public void shutdown() {
        super.shutdown();
        scheduledExecutor.shutdown();
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
                FirewallingMockConnectionManager.super.transmit(packet, connection);
            } else {
                FirewallingMockConnectionManager.super.transmit(packet, target);
            }
        }
    }
}

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

package com.hazelcast.internal.connection.tcp;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.connection.Connection;
import com.hazelcast.internal.connection.IOService;
import com.hazelcast.internal.connection.Packet;
import com.hazelcast.test.mocknetwork.MockConnectionManager;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FirewallingMockConnectionManager extends MockConnectionManager {

    private final Set<Address> blockedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private volatile PacketFilter packetFilter;

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

    public void setPacketFilter(PacketFilter packetFilter) {
        this.packetFilter = packetFilter;
    }

    private boolean isAllowed(Packet packet, Address target) {
        boolean allowed = true;
        PacketFilter filter = packetFilter;
        if (filter != null) {
            allowed = filter.allow(packet, target);
        }
        return allowed;
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
        return connection != null
                && isAllowed(packet, connection.getEndPoint())
                && super.transmit(packet, connection);
    }

    @Override
    public boolean transmit(Packet packet, Address target) {
        return isAllowed(packet, target) && super.transmit(packet, target);
    }
}

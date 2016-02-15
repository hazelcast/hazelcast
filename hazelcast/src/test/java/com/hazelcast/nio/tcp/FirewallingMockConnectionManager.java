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
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.mocknetwork.MockConnectionManager;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FirewallingMockConnectionManager extends MockConnectionManager {

    private final Set<Address> blockedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private volatile PacketFilter packetFilter;

    public FirewallingMockConnectionManager(IOService ioService, ConcurrentMap<Address, NodeEngineImpl> nodes, Node node, Object joinerLock) {
        super(ioService, nodes, node, joinerLock);
    }

    @Override
    public Connection getOrConnect(Address address, boolean silent) {
        Connection connection = getConnection(address);
        if (connection != null) {
            return connection;
        }
        if (blockedAddresses.contains(address)) {
            connection = new DroppingConnection(address);
            registerConnection(address, connection);
            return connection;
        }
        return super.getOrConnect(address, silent);
    }

    public void block(Address address) {
        blockedAddresses.add(address);
    }

    public void unblock(Address address) {
        blockedAddresses.remove(address);
        Connection connection = getConnection(address);
        if (connection instanceof DroppingConnection) {
            destroyConnection(connection);
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

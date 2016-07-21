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

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.NodeIOService;

import java.nio.channels.ServerSocketChannel;
import java.util.HashSet;
import java.util.Set;

public class FirewallingTcpIpConnectionManager extends TcpIpConnectionManager {

    private final Set<Address> blockedAddresses = new HashSet<Address>();

    public FirewallingTcpIpConnectionManager(
            LoggingService loggingService,
            HazelcastThreadGroup threadGroup,
            NodeIOService ioService,
            MetricsRegistry metricsRegistry,
            ServerSocketChannel serverSocketChannel) {
        super(ioService, serverSocketChannel, metricsRegistry, threadGroup, loggingService);
    }

    @Override
    public synchronized Connection getOrConnect(Address address, boolean silent) {
        Connection connection = getConnection(address);
        if (connection != null) {
            return connection;
        }
        if (blockedAddresses.contains(address)) {
            connection = new DroppingConnection(address, this);
            registerConnection(address, connection);
            return connection;
        }
        return super.getOrConnect(address, silent);
    }

    public synchronized void block(Address address) {
        blockedAddresses.add(address);
        Connection connection = getConnection(address);
        if (connection != null && connection instanceof TcpIpConnection) {
            connection.close("Blocked by connection manager", null);
        }
    }

    public synchronized void unblock(Address address) {
        blockedAddresses.remove(address);
        Connection connection = getConnection(address);
        if (connection instanceof DroppingConnection) {
            connection.close(null, null);
            onClose(connection);
        }
    }

}

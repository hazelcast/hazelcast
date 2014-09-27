/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.NodeIOService;

import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FirewallingTcpIpConnectionManager extends TcpIpConnectionManager {

    final Set<Address> blockedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    public FirewallingTcpIpConnectionManager(NodeIOService ioService, ServerSocketChannel serverSocketChannel,
            Node node) {
        super(ioService, serverSocketChannel, node.initializer);
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

}

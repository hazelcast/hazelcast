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
 *
 */

package com.hazelcast.test.mocknetwork;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.NodeExtensionFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

class MockNodeContext implements NodeContext {

    private final CopyOnWriteArrayList<Address> joinAddresses;
    private final ConcurrentMap<Address, NodeEngineImpl> nodes;
    private final Address thisAddress;
    private final Object joinerLock;

    public MockNodeContext(CopyOnWriteArrayList<Address> addresses, ConcurrentMap<Address, NodeEngineImpl> nodes,
            Address thisAddress, Object joinerLock) {
        this.joinAddresses = addresses;
        this.nodes = nodes;
        this.thisAddress = thisAddress;
        this.joinerLock = joinerLock;
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node);
    }

    public AddressPicker createAddressPicker(Node node) {
        return new StaticAddressPicker(thisAddress);
    }

    public Joiner createJoiner(Node node) {
        return new MockJoiner(node, joinAddresses, nodes, joinerLock);
    }

    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        return new FirewallingMockConnectionManager(ioService, nodes, node, joinerLock);
    }
}

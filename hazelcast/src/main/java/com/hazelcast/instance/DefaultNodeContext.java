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

package com.hazelcast.instance;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.IOThreadingModel;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.nio.tcp.spinning.SpinningIOThreadingModel;

import java.nio.channels.ServerSocketChannel;

public class DefaultNodeContext implements NodeContext {

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return new DefaultAddressPicker(node);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return node.createJoiner();
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        IOThreadingModel ioThreadingModel = createTcpIpConnectionThreadingModel(node, ioService);

        return new TcpIpConnectionManager(
                ioService,
                serverSocketChannel,
                node.loggingService,
                node.nodeEngine.getMetricsRegistry(),
                ioThreadingModel);
    }

    private IOThreadingModel createTcpIpConnectionThreadingModel(Node node, NodeIOService ioService) {
        boolean spinning = Boolean.getBoolean("hazelcast.io.spinning");
        if (spinning) {
            return new SpinningIOThreadingModel(
                    ioService,
                    node.loggingService,
                    node.nodeEngine.getMetricsRegistry(),
                    node.getHazelcastThreadGroup());
        } else {
            return new NonBlockingIOThreadingModel(
                    ioService,
                    node.loggingService,
                    node.nodeEngine.getMetricsRegistry(),
                    node.getHazelcastThreadGroup());
        }
    }
}

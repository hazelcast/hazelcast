/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.spinning.SpinningEventLoopGroup;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.MemberChannelInitializer;
import com.hazelcast.nio.tcp.TcpIpConnectionChannelErrorHandler;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.annotation.PrivateApi;

import java.nio.channels.ServerSocketChannel;

@PrivateApi
public class DefaultNodeContext implements NodeContext {

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return new DefaultAddressPicker(node.getConfig(), node.getProperties(), node.getLogger(AddressPicker.class));
    }

    @Override
    public Joiner createJoiner(Node node) {
        return node.createJoiner();
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        EventLoopGroup eventLoopGroup = createEventLoopGroup(node, ioService);

        return new TcpIpConnectionManager(
                ioService,
                serverSocketChannel,
                node.loggingService,
                node.nodeEngine.getMetricsRegistry(),
                eventLoopGroup);
    }

    private EventLoopGroup createEventLoopGroup(Node node, NodeIOService ioService) {
        boolean spinning = Boolean.getBoolean("hazelcast.io.spinning");
        LoggingServiceImpl loggingService = node.loggingService;

        MemberChannelInitializer initializer
                = new MemberChannelInitializer(loggingService.getLogger(MemberChannelInitializer.class), ioService);

        ChannelErrorHandler exceptionHandler
                = new TcpIpConnectionChannelErrorHandler(loggingService.getLogger(TcpIpConnectionChannelErrorHandler.class));

        if (spinning) {
            return new SpinningEventLoopGroup(
                    loggingService,
                    node.nodeEngine.getMetricsRegistry(),
                    exceptionHandler,
                    initializer,
                    node.hazelcastInstance.getName());
        } else {
            return new NioEventLoopGroup(
                    loggingService,
                    node.nodeEngine.getMetricsRegistry(),
                    node.hazelcastInstance.getName(),
                    exceptionHandler,
                    ioService.getInputSelectorThreadCount(),
                    ioService.getOutputSelectorThreadCount(),
                    ioService.getBalancerIntervalSeconds(),
                    initializer);
        }
    }

}

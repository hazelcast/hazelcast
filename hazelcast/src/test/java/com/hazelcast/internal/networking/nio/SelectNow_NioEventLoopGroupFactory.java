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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.tcp.EventLoopGroupFactory;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.MemberChannelInitializer;
import com.hazelcast.nio.tcp.TcpIpConnectionChannelErrorHandler;

public class SelectNow_NioEventLoopGroupFactory implements EventLoopGroupFactory {

    @Override
    public ChannelFactory createChannelFactory() {
        return new NioChannelFactory();
    }

    @Override
    public NioEventLoopGroup create(MockIOService ioService, MetricsRegistry metricsRegistry) {

        LoggingServiceImpl loggingService = ioService.loggingService;
        NioEventLoopGroup threadingModel = new NioEventLoopGroup(
                loggingService,
                metricsRegistry,
                ioService.getHazelcastName(),
                new TcpIpConnectionChannelErrorHandler(loggingService.getLogger(TcpIpConnectionChannelErrorHandler.class)),
                ioService.getInputSelectorThreadCount(),
                ioService.getOutputSelectorThreadCount(),
                ioService.getBalancerIntervalSeconds(),
                new MemberChannelInitializer(loggingService.getLogger(MemberChannelInitializer.class), ioService)
        );
        threadingModel.setSelectorMode(SelectorMode.SELECT_NOW);
        return threadingModel;
    }
}

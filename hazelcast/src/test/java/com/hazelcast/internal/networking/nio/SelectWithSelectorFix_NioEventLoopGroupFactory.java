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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.EventLoopGroupFactory;
import com.hazelcast.nio.tcp.MemberChannelInitializer;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.TcpIpConnectionChannelErrorHandler;

public class SelectWithSelectorFix_NioEventLoopGroupFactory implements EventLoopGroupFactory {

    @Override
    public ChannelFactory createChannelFactory() {
        return new NioChannelFactory();
    }

    @Override
    public NioEventLoopGroup create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
        return new NioEventLoopGroup(
                new NioEventLoopGroup.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(metricsRegistry)
                        .threadNamePrefix(ioService.getHazelcastName())
                        .errorHandler(
                                new TcpIpConnectionChannelErrorHandler(
                                        loggingService.getLogger(TcpIpConnectionChannelErrorHandler.class)))
                        .inputThreadCount(ioService.getInputSelectorThreadCount())
                        .outputThreadCount(ioService.getOutputSelectorThreadCount())
                        .balancerIntervalSeconds(ioService.getBalancerIntervalSeconds())
                        .channelInitializer(
                                new MemberChannelInitializer(
                                        loggingService.getLogger(MemberChannelInitializer.class), ioService))
                        .selectorMode(SelectorMode.SELECT_WITH_FIX)
                        .selectorWorkaroundTest(true));
    }
}

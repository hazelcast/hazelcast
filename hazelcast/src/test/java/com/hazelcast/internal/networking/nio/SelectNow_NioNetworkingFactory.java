/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.LoggingService;
import com.hazelcast.internal.nio.tcp.MockIOService;
import com.hazelcast.internal.nio.tcp.NetworkingFactory;
import com.hazelcast.internal.nio.tcp.TcpIpConnectionChannelErrorHandler;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.spi.properties.GroupProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.IO_OUTPUT_THREAD_COUNT;

public class SelectNow_NioNetworkingFactory implements NetworkingFactory {

    @Override
    public NioNetworking create(final MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
        HazelcastProperties properties = ioService.properties();
        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(metricsRegistry)
                        .threadNamePrefix(ioService.getHazelcastName())
                        .errorHandler(
                                new TcpIpConnectionChannelErrorHandler(
                                        loggingService.getLogger(TcpIpConnectionChannelErrorHandler.class)))
                        .inputThreadCount(properties.getInteger(IO_INPUT_THREAD_COUNT))
                        .outputThreadCount(properties.getInteger(IO_OUTPUT_THREAD_COUNT))
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .selectorMode(SelectorMode.SELECT_NOW));
    }
}

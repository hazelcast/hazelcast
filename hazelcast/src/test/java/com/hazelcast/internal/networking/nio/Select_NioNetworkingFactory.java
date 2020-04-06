/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.server.MockIOService;
import com.hazelcast.internal.nio.server.NetworkingFactory;
import com.hazelcast.internal.nio.server.ServerConnectionChannelErrorHandler;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.spi.properties.ClusterProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.IO_OUTPUT_THREAD_COUNT;

public class Select_NioNetworkingFactory implements NetworkingFactory {

    @Override
    public NioNetworking create(final MockIOService ioService, MetricsRegistry metricsRegistry) {
        HazelcastProperties properties = ioService.properties();
        LoggingService loggingService = ioService.loggingService;
        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(metricsRegistry)
                        .threadNamePrefix(ioService.getHazelcastName())
                        .errorHandler(
                                new ServerConnectionChannelErrorHandler(
                                        loggingService.getLogger(ServerConnectionChannelErrorHandler.class)))
                        .inputThreadCount(properties.getInteger(IO_INPUT_THREAD_COUNT))
                        .outputThreadCount(properties.getInteger(IO_OUTPUT_THREAD_COUNT))
                        .balancerIntervalSeconds(properties.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .selectorMode(SelectorMode.SELECT));
    }
}

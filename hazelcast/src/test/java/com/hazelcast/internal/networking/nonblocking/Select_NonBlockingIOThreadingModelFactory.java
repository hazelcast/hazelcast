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

package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.nio.tcp.SocketReaderInitializerImpl;
import com.hazelcast.nio.tcp.SocketWriterInitializerImpl;

public class Select_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                loggingService,
                metricsRegistry,
                ioService.getHazelcastName(),
                ioService.getIoOutOfMemoryHandler(), ioService.getInputSelectorThreadCount(),
                ioService.getOutputSelectorThreadCount(),
                ioService.getBalancerIntervalSeconds(),
                new SocketWriterInitializerImpl(loggingService.getLogger(SocketWriterInitializerImpl.class)),
                new SocketReaderInitializerImpl(loggingService.getLogger(SocketReaderInitializerImpl.class))
        );
        threadingModel.setSelectorMode(SelectorMode.SELECT);
        return threadingModel;
    }
}

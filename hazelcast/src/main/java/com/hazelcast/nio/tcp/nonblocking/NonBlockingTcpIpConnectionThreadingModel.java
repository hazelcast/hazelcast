/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.TcpIpConnectionThreadingModel;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.HashUtil.hashToIndex;

public class NonBlockingTcpIpConnectionThreadingModel implements TcpIpConnectionThreadingModel {

    private final InSelectorImpl[] inSelectors;
    private final OutSelectorImpl[] outSelectors;
    private final AtomicInteger nextInSelectorIndex = new AtomicInteger();
    private final AtomicInteger nextOutSelectorIndex = new AtomicInteger();
    private final ILogger logger;
    private final IOService ioService;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final HazelcastThreadGroup hazelcastThreadGroup;
    private volatile IOBalancer ioBalancer;

    public NonBlockingTcpIpConnectionThreadingModel(
            IOService ioService,
            LoggingService loggingService,
            MetricsRegistry metricsRegistry,
            HazelcastThreadGroup hazelcastThreadGroup) {
        this.ioService = ioService;
        this.hazelcastThreadGroup = hazelcastThreadGroup;
        this.metricsRegistry = metricsRegistry;
        this.loggingService = loggingService;
        this.logger = ioService.getLogger(NonBlockingTcpIpConnectionThreadingModel.class.getName());
        this.inSelectors = new InSelectorImpl[ioService.getInputSelectorThreadCount()];
        this.outSelectors = new OutSelectorImpl[ioService.getOutputSelectorThreadCount()];

        logger.info("TcpIpConnection managed configured with "
                + inSelectors.length + " input threads and "
                + outSelectors.length + " output threads");
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public InSelectorImpl[] getInSelectors() {
        return inSelectors;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public OutSelectorImpl[] getOutSelectors() {
        return outSelectors;
    }

    public IOBalancer getIOBalancer() {
        return ioBalancer;
    }

    @Override
    public void start() {
        IOSelectorOutOfMemoryHandler oomeHandler = new IOSelectorOutOfMemoryHandler() {
            @Override
            public void handle(OutOfMemoryError error) {
                ioService.onOutOfMemory(error);
            }
        };

        for (int i = 0; i < inSelectors.length; i++) {
            InSelectorImpl inSelector = new InSelectorImpl(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "in-" + i,
                    ioService.getLogger(InSelectorImpl.class.getName()),
                    oomeHandler
            );
            inSelectors[i] = inSelector;
            metricsRegistry.scanAndRegister(inSelector, "tcp." + inSelector.getName());
            inSelector.start();
        }

        for (int i = 0; i < outSelectors.length; i++) {
            OutSelectorImpl outSelector = new OutSelectorImpl(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "out-" + i,
                    ioService.getLogger(OutSelectorImpl.class.getName()),
                    oomeHandler);
            outSelectors[i] = outSelector;
            metricsRegistry.scanAndRegister(outSelector, "tcp." + outSelector.getName());
            outSelector.start();
        }
        startIOBalancer();
    }

    @Override
    public void onConnectionAdded(TcpIpConnection connection) {
        ioBalancer.connectionAdded(connection);
    }

    @Override
    public void onConnectionRemoved(TcpIpConnection connection) {
        ioBalancer.connectionRemoved(connection);
    }

    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inSelectors, outSelectors,
                hazelcastThreadGroup, ioService.getBalancerIntervalSeconds(), loggingService);
        ioBalancer.start();
        metricsRegistry.scanAndRegister(ioBalancer, "tcp.balancer");
    }

    @Override
    public void shutdown() {
        ioBalancer.stop();

        if (logger.isFinestEnabled()) {
            logger.finest("Shutting down IO selectors... Total: " + (inSelectors.length + outSelectors.length));
        }

        for (int i = 0; i < inSelectors.length; i++) {
            IOSelector ioSelector = inSelectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            inSelectors[i] = null;
        }

        for (int i = 0; i < outSelectors.length; i++) {
            IOSelector ioSelector = outSelectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            outSelectors[i] = null;
        }
    }

    @Override
    public WriteHandler newWriteHandler(TcpIpConnection connection) {
        int index = hashToIndex(nextOutSelectorIndex.getAndIncrement(), outSelectors.length);
        return new NonBlockingWriteHandler(connection, outSelectors[index], metricsRegistry);
    }

    @Override
    public ReadHandler newReadHandler(TcpIpConnection connection) {
        int index = hashToIndex(nextInSelectorIndex.getAndIncrement(), inSelectors.length);
        return new NonBlockingReadHandler(connection, inSelectors[index], metricsRegistry);
    }
}

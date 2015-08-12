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
import com.hazelcast.internal.metrics.CompositeProbe;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeTraverse;
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

@CompositeProbe
public class NonBlockingTcpIpConnectionThreadingModel implements TcpIpConnectionThreadingModel {

    @ProbeTraverse
    private final NonBlockingInputThread[] inputThreads;
    @ProbeTraverse
    private final NonBlockingOutputThread[] outputThreads;
    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private final AtomicInteger nextOutputThreadIndex = new AtomicInteger();
    private final ILogger logger;
    private final IOService ioService;
    private final LoggingService loggingService;
    private final HazelcastThreadGroup hazelcastThreadGroup;
    @ProbeTraverse
    private volatile IOBalancer ioBalancer;

    public NonBlockingTcpIpConnectionThreadingModel(
            IOService ioService,
            LoggingService loggingService,
            HazelcastThreadGroup hazelcastThreadGroup) {
        this.ioService = ioService;
        this.hazelcastThreadGroup = hazelcastThreadGroup;
        this.loggingService = loggingService;
        this.logger = ioService.getLogger(NonBlockingTcpIpConnectionThreadingModel.class.getName());
        this.inputThreads = new NonBlockingInputThread[ioService.getInputSelectorThreadCount()];
        this.outputThreads = new NonBlockingOutputThread[ioService.getOutputSelectorThreadCount()];

        logger.info("TcpIpConnection managed configured with "
                + inputThreads.length + " input threads and "
                + outputThreads.length + " output threads");
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NonBlockingInputThread[] getInputThreads() {
        return inputThreads;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NonBlockingOutputThread[] getOutputThreads() {
        return outputThreads;
    }

    public IOBalancer getIOBalancer() {
        return ioBalancer;
    }

    @Override
    public void start() {
        NonBlockingIOThreadOutOfMemoryHandler oomeHandler = new NonBlockingIOThreadOutOfMemoryHandler() {
            @Override
            public void handle(OutOfMemoryError error) {
                ioService.onOutOfMemory(error);
            }
        };

        for (int i = 0; i < inputThreads.length; i++) {
            NonBlockingInputThread thread = new NonBlockingInputThread(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "in-" + i,
                    ioService.getLogger(NonBlockingInputThread.class.getName()),
                    oomeHandler
            );
            inputThreads[i] = thread;
            thread.start();
        }

        for (int i = 0; i < outputThreads.length; i++) {
            NonBlockingOutputThread thread = new NonBlockingOutputThread(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "out-" + i,
                    ioService.getLogger(NonBlockingOutputThread.class.getName()),
                    oomeHandler);
            outputThreads[i] = thread;
            thread.start();
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
        ioBalancer = new IOBalancer(inputThreads, outputThreads,
                hazelcastThreadGroup, ioService.getBalancerIntervalSeconds(), loggingService);
        ioBalancer.start();
    }

    @Override
    public void shutdown() {
        ioBalancer.stop();

        if (logger.isFinestEnabled()) {
            logger.finest("Shutting down IO selectors... Total: " + (inputThreads.length + outputThreads.length));
        }

        for (int i = 0; i < inputThreads.length; i++) {
            NonBlockingIOThread ioThread = inputThreads[i];
            if (ioThread != null) {
                ioThread.shutdown();
            }
            inputThreads[i] = null;
        }

        for (int i = 0; i < outputThreads.length; i++) {
            NonBlockingIOThread ioThread = outputThreads[i];
            if (ioThread != null) {
                ioThread.shutdown();
            }
            outputThreads[i] = null;
        }
    }

    @Override
    public WriteHandler newWriteHandler(TcpIpConnection connection) {
        int index = hashToIndex(nextOutputThreadIndex.getAndIncrement(), outputThreads.length);
        return new NonBlockingWriteHandler(connection, outputThreads[index]);
    }

    @Override
    public ReadHandler newReadHandler(TcpIpConnection connection) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreads.length);
        return new NonBlockingReadHandler(connection, inputThreads[index]);
    }
}

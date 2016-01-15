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
import com.hazelcast.nio.tcp.IOThreadingModel;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.tcp.nonblocking.PacketSampler.isPacketSamplerEnabled;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static java.lang.Boolean.getBoolean;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;

/**
 * A non blocking {@link IOThreadingModel} implementation that makes use of {@link java.nio.channels.Selector} to have a
 * limited set of io threads, handle an arbitrary number of connections.
 *
 * By default the {@link NonBlockingIOThread} blocks on the Selector, but it can be put in a 'selectNow' mode that makes it
 * spinning on the selector. This is an experimental feature and will cause the io threads to run hot. For this reason, when
 * this feature is enabled, the number of io threads should be reduced (preferably 1).
 */
public class NonBlockingIOThreadingModel implements IOThreadingModel {

    private final NonBlockingIOThread[] inputThreads;
    private final NonBlockingIOThread[] outputThreads;
    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private final AtomicInteger nextOutputThreadIndex = new AtomicInteger();
    private final ILogger logger;
    private final IOService ioService;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final HazelcastThreadGroup hazelcastThreadGroup;
    // experimental settings; will be disabled by default.
    private boolean inputSelectNow = getBoolean("hazelcast.io.input.thread.selectNow");
    private boolean outputSelectNow = getBoolean("hazelcast.io.output.thread.selectNow");
    private volatile IOBalancer ioBalancer;
    private volatile PacketSampler packetSampler;

    public NonBlockingIOThreadingModel(
            IOService ioService,
            LoggingService loggingService,
            MetricsRegistry metricsRegistry,
            HazelcastThreadGroup hazelcastThreadGroup) {
        this.ioService = ioService;
        this.hazelcastThreadGroup = hazelcastThreadGroup;
        this.metricsRegistry = metricsRegistry;
        this.loggingService = loggingService;
        this.logger = loggingService.getLogger(NonBlockingIOThreadingModel.class);
        this.inputThreads = new NonBlockingIOThread[ioService.getInputSelectorThreadCount()];
        this.outputThreads = new NonBlockingIOThread[ioService.getOutputSelectorThreadCount()];
    }

    public void setInputSelectNow(boolean enabled) {
        this.inputSelectNow = enabled;
    }

    public void setOutputSelectNow(boolean enabled) {
        this.outputSelectNow = enabled;
    }

    @Override
    public boolean isBlocking() {
        return false;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NonBlockingIOThread[] getInputThreads() {
        return inputThreads;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NonBlockingIOThread[] getOutputThreads() {
        return outputThreads;
    }

    public IOBalancer getIOBalancer() {
        return ioBalancer;
    }

    @Override
    public void start() {
        logger.info("TcpIpConnectionManager configured with Non Blocking IO-threading model: "
                + inputThreads.length + " input threads and "
                + outputThreads.length + " output threads");

        logger.log(inputSelectNow ? INFO : FINE, "InputThreads selectNow enabled=" + inputSelectNow);
        logger.log(outputSelectNow ? INFO : FINE, "OutputThreads selectNow enabled=" + outputSelectNow);

        NonBlockingIOThreadOutOfMemoryHandler oomeHandler = new NonBlockingIOThreadOutOfMemoryHandler() {
            @Override
            public void handle(OutOfMemoryError error) {
                ioService.onOutOfMemory(error);
            }
        };

        for (int i = 0; i < inputThreads.length; i++) {
            NonBlockingIOThread thread = new NonBlockingIOThread(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "in-" + i,
                    ioService.getLogger(NonBlockingIOThread.class.getName()),
                    oomeHandler,
                    inputSelectNow
            );
            inputThreads[i] = thread;
            metricsRegistry.scanAndRegister(thread, "tcp." + thread.getName());
            thread.start();
        }

        for (int i = 0; i < outputThreads.length; i++) {
            NonBlockingIOThread thread = new NonBlockingIOThread(
                    ioService.getThreadGroup(),
                    ioService.getThreadPrefix() + "out-" + i,
                    ioService.getLogger(NonBlockingIOThread.class.getName()),
                    oomeHandler,
                    outputSelectNow);
            outputThreads[i] = thread;
            metricsRegistry.scanAndRegister(thread, "tcp." + thread.getName());
            thread.start();
        }
        startIOBalancer();

        if (isPacketSamplerEnabled()) {
            packetSampler = new PacketSampler(hazelcastThreadGroup, ioService);
            packetSampler.start();
        }
    }

    @Override
    public void onConnectionAdded(TcpIpConnection connection) {
        ioBalancer.connectionAdded(connection);
        if (packetSampler != null) {
            packetSampler.onConnectionAdded(connection);
        }
    }

    @Override
    public void onConnectionRemoved(TcpIpConnection connection) {
        ioBalancer.connectionRemoved(connection);
        if (packetSampler != null) {
            packetSampler.onConnectionRemoved(connection);
        }
    }

    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inputThreads, outputThreads,
                hazelcastThreadGroup, ioService.getBalancerIntervalSeconds(), loggingService);
        ioBalancer.start();
        metricsRegistry.scanAndRegister(ioBalancer, "tcp.balancer");
    }

    @Override
    public void shutdown() {
        ioBalancer.stop();

        if (logger.isFinestEnabled()) {
            logger.finest("Shutting down IO selectors... Total: " + (inputThreads.length + outputThreads.length));
        }

        shutdown(inputThreads);
        shutdown(outputThreads);

        if (packetSampler != null) {
            packetSampler.shutdown();
        }
    }

    private void shutdown(NonBlockingIOThread[] threads) {
        for (int i = 0; i < threads.length; i++) {
            NonBlockingIOThread ioThread = threads[i];
            if (ioThread != null) {
                ioThread.shutdown();
            }
            threads[i] = null;
        }
    }

    @Override
    public SocketWriter newSocketWriter(TcpIpConnection connection) {
        int index = hashToIndex(nextOutputThreadIndex.getAndIncrement(), outputThreads.length);
        return new NonBlockingSocketWriter(connection, outputThreads[index], metricsRegistry);
    }

    @Override
    public SocketReader newSocketReader(TcpIpConnection connection) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreads.length);
        return new NonBlockingSocketReader(connection, inputThreads[index], metricsRegistry);
    }
}

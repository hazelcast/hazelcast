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
import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.internal.networking.IOThreadingModel;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.SocketReader;
import com.hazelcast.internal.networking.SocketReaderInitializer;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.internal.networking.SocketWriterInitializer;
import com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.networking.nonblocking.SelectorMode.SELECT;
import static com.hazelcast.internal.networking.nonblocking.SelectorMode.SELECT_NOW_STRING;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.util.concurrent.BackoffIdleStrategy.createBackoffIdleStrategy;
import static java.util.concurrent.TimeUnit.SECONDS;
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
public class NonBlockingIOThreadingModel
        implements IOThreadingModel {

    private volatile NonBlockingIOThread[] inputThreads;
    private volatile NonBlockingIOThread[] outputThreads;
    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private final AtomicInteger nextOutputThreadIndex = new AtomicInteger();
    private final ILogger logger;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final String hzName;
    private final IOOutOfMemoryHandler oomeHandler;
    private final int balanceIntervalSeconds;
    private final SocketWriterInitializer socketWriterInitializer;
    private final SocketReaderInitializer socketReaderInitializer;
    private final int inputThreadCount;
    private final int outputThreadCount;
    private final Map<SocketConnection, SocketConnection> connections
            = new ConcurrentHashMap<SocketConnection, SocketConnection>();

    // The selector mode determines how IO threads will block (or not) on the Selector:
    //  select:         this is the default mode, uses Selector.select(long timeout)
    //  selectnow:      use Selector.selectNow()
    //  selectwithfix:  use Selector.select(timeout) with workaround for bug occurring when
    //                  SelectorImpl.select returns immediately with no channels selected,
    //                  resulting in 100% CPU usage while doing no progress.
    // See issue: https://github.com/hazelcast/hazelcast/issues/7943
    // In Hazelcast 3.8, selector mode must be set via HazelcastProperties
    private SelectorMode selectorMode;
    private BackoffIdleStrategy idleStrategy;
    private volatile IOBalancer ioBalancer;
    private boolean selectorWorkaroundTest = Boolean.getBoolean("hazelcast.io.selector.workaround.test");

    public NonBlockingIOThreadingModel(
            LoggingService loggingService,
            MetricsRegistry metricsRegistry,
            String hzName,
            IOOutOfMemoryHandler oomeHandler,
            int inputThreadCount,
            int outputThreadCount,
            int balanceIntervalSeconds,
            SocketWriterInitializer socketWriterInitializer,
            SocketReaderInitializer socketReaderInitializer) {
        this.hzName = hzName;
        this.metricsRegistry = metricsRegistry;
        this.loggingService = loggingService;
        this.inputThreadCount = inputThreadCount;
        this.outputThreadCount = outputThreadCount;
        this.logger = loggingService.getLogger(NonBlockingIOThreadingModel.class);
        this.oomeHandler = oomeHandler;
        this.balanceIntervalSeconds = balanceIntervalSeconds;
        this.socketWriterInitializer = socketWriterInitializer;
        this.socketReaderInitializer = socketReaderInitializer;
    }

    private SelectorMode getSelectorMode() {
        if (selectorMode == null) {
            selectorMode = SelectorMode.getConfiguredValue();

            String selectorModeString = SelectorMode.getConfiguredString();
            if (selectorModeString.startsWith(SELECT_NOW_STRING + ",")) {
                idleStrategy = createBackoffIdleStrategy(selectorModeString);
            }
        }
        return selectorMode;
    }

    public void setSelectorMode(SelectorMode mode) {
        this.selectorMode = mode;
    }

    /**
     * Set to {@code true} for Selector CPU-consuming bug workaround tests
     *
     * @param selectorWorkaroundTest
     */
    void setSelectorWorkaroundTest(boolean selectorWorkaroundTest) {
        this.selectorWorkaroundTest = selectorWorkaroundTest;
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
        if (logger.isFineEnabled()) {
            logger.fine("TcpIpConnectionManager configured with Non Blocking IO-threading model: "
                    + inputThreadCount + " input threads and "
                    + outputThreadCount + " output threads");
        }


        logger.log(getSelectorMode() != SELECT ? INFO : FINE, "IO threads selector mode is " + getSelectorMode());
        this.inputThreads = new NonBlockingIOThread[inputThreadCount];

        for (int i = 0; i < inputThreads.length; i++) {
            NonBlockingIOThread thread = new NonBlockingIOThread(
                    createThreadPoolName(hzName, "IO") + "in-" + i,
                    loggingService.getLogger(NonBlockingIOThread.class),
                    oomeHandler,
                    selectorMode,
                    idleStrategy);
            thread.id = i;
            thread.setSelectorWorkaroundTest(selectorWorkaroundTest);
            inputThreads[i] = thread;
            metricsRegistry.scanAndRegister(thread, "tcp.inputThread[" + thread.getName() + "]");
            thread.start();
        }

        this.outputThreads = new NonBlockingIOThread[outputThreadCount];
        for (int i = 0; i < outputThreads.length; i++) {
            NonBlockingIOThread thread = new NonBlockingIOThread(
                    createThreadPoolName(hzName, "IO") + "out-" + i,
                    loggingService.getLogger(NonBlockingIOThread.class),
                    oomeHandler,
                    selectorMode,
                    idleStrategy);
            thread.id = i;
            thread.setSelectorWorkaroundTest(selectorWorkaroundTest);
            outputThreads[i] = thread;
            metricsRegistry.scanAndRegister(thread, "tcp.outputThread[" + thread.getName() + "]");
            thread.start();
        }
        startIOBalancer();

        if (metricsRegistry.minimumLevel().isEnabled(DEBUG)) {
            metricsRegistry.scheduleAtFixedRate(new PublishAllTask(), 1, SECONDS);
        }
    }

    private class PublishAllTask implements Runnable {
        @Override
        public void run() {
            for (SocketConnection connection : connections.values()) {
                final NonBlockingSocketReader reader = (NonBlockingSocketReader) connection.getSocketReader();
                NonBlockingIOThread inputThread = reader.getOwner();
                if (inputThread != null) {
                    inputThread.addTaskAndWakeup(new Runnable() {
                        @Override
                        public void run() {
                            reader.publish();
                        }
                    });
                }

                final NonBlockingSocketWriter writer = (NonBlockingSocketWriter) connection.getSocketWriter();
                NonBlockingIOThread outputThread = writer.getOwner();
                if (outputThread != null) {
                    outputThread.addTaskAndWakeup(new Runnable() {
                        @Override
                        public void run() {
                            writer.publish();
                        }
                    });
                }
            }
        }
    }

    @Override
    public void onConnectionAdded(SocketConnection connection) {
        connections.put(connection, connection);

        MigratableHandler reader = (MigratableHandler) connection.getSocketReader();
        MigratableHandler writer = (MigratableHandler) connection.getSocketWriter();
        ioBalancer.connectionAdded(reader, writer);
    }

    @Override
    public void onConnectionRemoved(SocketConnection connection) {
        connections.remove(connection);

        MigratableHandler reader = (MigratableHandler) connection.getSocketReader();
        MigratableHandler writer = (MigratableHandler) connection.getSocketWriter();
        ioBalancer.connectionRemoved(reader, writer);
    }

    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inputThreads, outputThreads, hzName, balanceIntervalSeconds, loggingService);
        ioBalancer.start();
        metricsRegistry.scanAndRegister(ioBalancer, "tcp.balancer");
    }

    @Override
    public void shutdown() {
        ioBalancer.stop();

        if (logger.isFinestEnabled()) {
            logger.finest("Shutting down IO Threads... Total: " + (inputThreads.length + outputThreads.length));
        }

        shutdown(inputThreads);
        inputThreads = null;
        shutdown(outputThreads);
        outputThreads = null;
    }

    private void shutdown(NonBlockingIOThread[] threads) {
        if (threads == null) {
            return;
        }
        for (NonBlockingIOThread thread : threads) {
            thread.shutdown();
        }
    }

    @Override
    public SocketWriter newSocketWriter(SocketConnection connection) {
        int index = hashToIndex(nextOutputThreadIndex.getAndIncrement(), outputThreadCount);
        NonBlockingIOThread[] threads = outputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NonBlockingSocketWriter(
                connection,
                threads[index],
                loggingService.getLogger(NonBlockingSocketWriter.class),
                ioBalancer,
                socketWriterInitializer);
    }

    @Override
    public SocketReader newSocketReader(SocketConnection connection) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreadCount);
        NonBlockingIOThread[] threads = inputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NonBlockingSocketReader(
                connection,
                threads[index],
                loggingService.getLogger(NonBlockingSocketReader.class),
                ioBalancer,
                socketReaderInitializer);
    }
}

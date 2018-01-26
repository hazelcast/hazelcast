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
import com.hazelcast.internal.networking.ChannelCloseListener;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_NOW_STRING;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static com.hazelcast.util.Preconditions.checkInstanceOf;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.util.concurrent.BackoffIdleStrategy.createBackoffIdleStrategy;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;

/**
 * A non blocking {@link EventLoopGroup} implementation that makes use of {@link java.nio.channels.Selector} to have a
 * limited set of io threads, handle an arbitrary number of connections.
 *
 * Each {@link NioChannel} has 2 parts:
 * <ol>
 * <li>{@link NioChannelReader}: triggered by the NioThread when data is available in the socket. The NioChannelReader
 * takes care of reading data from the socket and calling the appropriate
 * {@link com.hazelcast.internal.networking.ChannelInboundHandler}</li>
 * <li>{@link NioChannelWriter}: triggered by the NioThread when either space is available in the socket for writing,
 * or when there is something that needs to be written e.g. a Packet. The NioChannelWriter takes care of calling the
 * appropriate {@link com.hazelcast.internal.networking.ChannelOutboundHandler} to convert the
 * {@link com.hazelcast.internal.networking.OutboundFrame} to bytes in in the ByteBuffer and writing it to the socket.
 * </li>
 * </ol>
 *
 * By default the {@link NioThread} blocks on the Selector, but it can be put in a 'selectNow' mode that makes it
 * spinning on the selector. This is an experimental feature and will cause the io threads to run hot. For this reason, when
 * this feature is enabled, the number of io threads should be reduced (preferably 1).
 */
public class NioEventLoopGroup
        implements EventLoopGroup {

    private volatile NioThread[] inputThreads;
    private volatile NioThread[] outputThreads;
    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private final AtomicInteger nextOutputThreadIndex = new AtomicInteger();
    private final ILogger logger;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final String hzName;
    private final ChannelErrorHandler errorHandler;
    private final int balanceIntervalSeconds;
    private final ChannelInitializer channelInitializer;
    private final int inputThreadCount;
    private final int outputThreadCount;
    private final Set<NioChannel> channels = newSetFromMap(new ConcurrentHashMap<NioChannel, Boolean>());
    private final ChannelCloseListener channelCloseListener = new ChannelCloseListenerImpl();

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

    public NioEventLoopGroup(
            LoggingService loggingService,
            MetricsRegistry metricsRegistry,
            String hzName,
            ChannelErrorHandler errorHandler,
            int inputThreadCount,
            int outputThreadCount,
            int balanceIntervalSeconds,
            ChannelInitializer channelInitializer) {
        this.hzName = hzName;
        this.metricsRegistry = metricsRegistry;
        this.loggingService = loggingService;
        this.inputThreadCount = inputThreadCount;
        this.outputThreadCount = outputThreadCount;
        this.logger = loggingService.getLogger(NioEventLoopGroup.class);
        this.errorHandler = errorHandler;
        this.balanceIntervalSeconds = balanceIntervalSeconds;
        this.channelInitializer = channelInitializer;
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

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NioThread[] getInputThreads() {
        return inputThreads;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NioThread[] getOutputThreads() {
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
        this.inputThreads = new NioThread[inputThreadCount];

        for (int i = 0; i < inputThreads.length; i++) {
            NioThread thread = new NioThread(
                    createThreadPoolName(hzName, "IO") + "in-" + i,
                    loggingService.getLogger(NioThread.class),
                    errorHandler,
                    selectorMode,
                    idleStrategy);
            thread.id = i;
            thread.setSelectorWorkaroundTest(selectorWorkaroundTest);
            inputThreads[i] = thread;
            metricsRegistry.scanAndRegister(thread, "tcp.inputThread[" + thread.getName() + "]");
            thread.start();
        }

        this.outputThreads = new NioThread[outputThreadCount];
        for (int i = 0; i < outputThreads.length; i++) {
            NioThread thread = new NioThread(
                    createThreadPoolName(hzName, "IO") + "out-" + i,
                    loggingService.getLogger(NioThread.class),
                    errorHandler,
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
            for (NioChannel channel : channels) {
                final NioChannelReader reader = channel.getReader();
                NioThread inputThread = reader.getOwner();
                if (inputThread != null) {
                    inputThread.addTaskAndWakeup(new Runnable() {
                        @Override
                        public void run() {
                            reader.publish();
                        }
                    });
                }

                final NioChannelWriter writer = channel.getWriter();
                NioThread outputThread = writer.getOwner();
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

    private void shutdown(NioThread[] threads) {
        if (threads == null) {
            return;
        }
        for (NioThread thread : threads) {
            thread.shutdown();
        }
    }

    @Override
    public void register(final Channel channel) {
        NioChannel nioChannel = checkInstanceOf(NioChannel.class, channel);

        try {
            nioChannel.socketChannel().configureBlocking(false);
        } catch (IOException e) {
            throw rethrow(e);
        }

        NioChannelReader reader = newChannelReader(nioChannel);
        NioChannelWriter writer = newChannelWriter(nioChannel);

        channels.add(nioChannel);

        nioChannel.setReader(reader);
        nioChannel.setWriter(writer);

        ioBalancer.channelAdded(reader, writer);

        String metricsId = channel.getLocalSocketAddress() + "->" + channel.getRemoteSocketAddress();
        metricsRegistry.scanAndRegister(writer, "tcp.connection[" + metricsId + "].out");
        metricsRegistry.scanAndRegister(reader, "tcp.connection[" + metricsId + "].in");

        reader.start();
        writer.start();

        channel.addCloseListener(channelCloseListener);
    }

    private NioChannelWriter newChannelWriter(NioChannel channel) {
        int index = hashToIndex(nextOutputThreadIndex.getAndIncrement(), outputThreadCount);
        NioThread[] threads = outputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NioChannelWriter(
                channel,
                threads[index],
                loggingService.getLogger(NioChannelWriter.class),
                ioBalancer,
                channelInitializer);
    }

    private NioChannelReader newChannelReader(NioChannel channel) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreadCount);
        NioThread[] threads = inputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NioChannelReader(
                channel,
                threads[index],
                loggingService.getLogger(NioChannelReader.class),
                ioBalancer,
                channelInitializer);
    }

    private class ChannelCloseListenerImpl implements ChannelCloseListener {
        @Override
        public void onClose(Channel channel) {
            NioChannel nioChannel = (NioChannel) channel;

            channels.remove(channel);

            ioBalancer.channelRemoved(nioChannel.getReader(), nioChannel.getWriter());

            metricsRegistry.deregister(nioChannel.getReader());
            metricsRegistry.deregister(nioChannel.getWriter());
        }
    }
}

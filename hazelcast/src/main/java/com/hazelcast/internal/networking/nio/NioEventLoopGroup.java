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
 * A non blocking {@link EventLoopGroup} implementation that makes use of
 * {@link java.nio.channels.Selector} to have a limited set of io threads, handle
 * an arbitrary number of connections.
 *
 * Each {@link NioChannel} has 2 parts:
 * <ol>
 * <li>{@link NioInboundPipeline}: triggered by the NioThread when data is available
 * in the socket. The NioInboundPipeline takes care of reading data from the socket
 * and calling the appropriate
 * {@link com.hazelcast.internal.networking.ChannelInboundHandler}</li>
 * <li>{@link NioOutboundPipeline}: triggered by the NioThread when either space
 * is available in the socket for writing, or when there is something that needs to
 * be written e.g. a Packet. The NioOutboundPipeline takes care of calling the
 * appropriate {@link com.hazelcast.internal.networking.ChannelOutboundHandler}
 * to convert the {@link com.hazelcast.internal.networking.OutboundFrame} to bytes
 * in in the ByteBuffer and writing it to the socket.
 * </li>
 * </ol>
 *
 * By default the {@link NioThread} blocks on the Selector, but it can be put in a
 * 'selectNow' mode that makes it spinning on the selector. This is an experimental
 * feature and will cause the io threads to run hot. For this reason, when this feature
 * is enabled, the number of io threads should be reduced (preferably 1).
 */
public final class NioEventLoopGroup implements EventLoopGroup {

    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private final AtomicInteger nextOutputThreadIndex = new AtomicInteger();
    private final ILogger logger;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final String threadNamePrefix;
    private final ChannelErrorHandler errorHandler;
    private final int balancerIntervalSeconds;
    private final ChannelInitializer channelInitializer;
    private final int inputThreadCount;
    private final int outputThreadCount;
    private final Set<NioChannel> channels = newSetFromMap(new ConcurrentHashMap<NioChannel, Boolean>());
    private final ChannelCloseListener channelCloseListener = new ChannelCloseListenerImpl();
    private final SelectorMode selectorMode;
    private final BackoffIdleStrategy idleStrategy;
    private final boolean selectorWorkaroundTest;
    private volatile IOBalancer ioBalancer;
    private volatile NioThread[] inputThreads;
    private volatile NioThread[] outputThreads;

    public NioEventLoopGroup(Context ctx) {
        this.threadNamePrefix = ctx.threadNamePrefix;
        this.metricsRegistry = ctx.metricsRegistry;
        this.loggingService = ctx.loggingService;
        this.inputThreadCount = ctx.inputThreadCount;
        this.outputThreadCount = ctx.outputThreadCount;
        this.logger = loggingService.getLogger(NioEventLoopGroup.class);
        this.errorHandler = ctx.errorHandler;
        this.balancerIntervalSeconds = ctx.balancerIntervalSeconds;
        this.channelInitializer = ctx.channelInitializer;
        this.selectorMode = ctx.selectorMode;
        this.selectorWorkaroundTest = ctx.selectorWorkaroundTest;
        this.idleStrategy = ctx.idleStrategy;
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

        logger.log(selectorMode != SELECT ? INFO : FINE, "IO threads selector mode is " + selectorMode);

        this.inputThreads = new NioThread[inputThreadCount];
        for (int i = 0; i < inputThreads.length; i++) {
            NioThread thread = new NioThread(
                    createThreadPoolName(threadNamePrefix, "IO") + "in-" + i,
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
                    createThreadPoolName(threadNamePrefix, "IO") + "out-" + i,
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

    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inputThreads, outputThreads, threadNamePrefix, balancerIntervalSeconds, loggingService);
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

        NioInboundPipeline inboundPipeline = newInboundPipeline(nioChannel);
        NioOutboundPipeline outboundPipeline = newOutboundPipeline(nioChannel);

        channels.add(nioChannel);

        nioChannel.init(inboundPipeline, outboundPipeline);

        ioBalancer.channelAdded(inboundPipeline, outboundPipeline);

        String metricsId = channel.localSocketAddress() + "->" + channel.remoteSocketAddress();
        metricsRegistry.scanAndRegister(outboundPipeline, "tcp.connection[" + metricsId + "].out");
        metricsRegistry.scanAndRegister(inboundPipeline, "tcp.connection[" + metricsId + "].in");

        inboundPipeline.start();
        outboundPipeline.start();

        channel.addCloseListener(channelCloseListener);
    }

    private NioOutboundPipeline newOutboundPipeline(NioChannel channel) {
        int index = hashToIndex(nextOutputThreadIndex.getAndIncrement(), outputThreadCount);
        NioThread[] threads = outputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NioOutboundPipeline(
                channel,
                threads[index],
                errorHandler,
                loggingService.getLogger(NioOutboundPipeline.class),
                ioBalancer,
                channelInitializer);
    }

    private NioInboundPipeline newInboundPipeline(NioChannel channel) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreadCount);
        NioThread[] threads = inputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NioInboundPipeline(
                channel,
                threads[index],
                errorHandler,
                loggingService.getLogger(NioInboundPipeline.class),
                ioBalancer,
                channelInitializer);
    }

    private class ChannelCloseListenerImpl implements ChannelCloseListener {
        @Override
        public void onClose(Channel channel) {
            NioChannel nioChannel = (NioChannel) channel;

            channels.remove(channel);

            ioBalancer.channelRemoved(nioChannel.inboundPipeline, nioChannel.outboundPipeline);

            metricsRegistry.deregister(nioChannel.inboundPipeline);
            metricsRegistry.deregister(nioChannel.outboundPipeline);
        }
    }

    private class PublishAllTask implements Runnable {
        @Override
        public void run() {
            for (NioChannel channel : channels) {
                final NioInboundPipeline inboundPipeline = channel.inboundPipeline;
                NioThread inputThread = inboundPipeline.owner();
                if (inputThread != null) {
                    inputThread.addTaskAndWakeup(new Runnable() {
                        @Override
                        public void run() {
                            inboundPipeline.publishMetrics();
                        }
                    });
                }

                final NioOutboundPipeline outboundPipeline = channel.outboundPipeline;
                NioThread outputThread = outboundPipeline.owner();
                if (outputThread != null) {
                    outputThread.addTaskAndWakeup(new Runnable() {
                        @Override
                        public void run() {
                            outboundPipeline.publishMetrics();
                        }
                    });
                }
            }
        }
    }

    public static class Context {
        private BackoffIdleStrategy idleStrategy;
        private LoggingService loggingService;
        private MetricsRegistry metricsRegistry;
        private String threadNamePrefix = "hz";
        private ChannelErrorHandler errorHandler;
        private int inputThreadCount = 1;
        private int outputThreadCount = 1;
        private int balancerIntervalSeconds;
        // The selector mode determines how IO threads will block (or not) on the Selector:
        //  select:         this is the default mode, uses Selector.select(long timeout)
        //  selectnow:      use Selector.selectNow()
        //  selectwithfix:  use Selector.select(timeout) with workaround for bug occurring when
        //                  SelectorImpl.select returns immediately with no channels selected,
        //                  resulting in 100% CPU usage while doing no progress.
        // See issue: https://github.com/hazelcast/hazelcast/issues/7943
        // In Hazelcast 3.8, selector mode must be set via HazelcastProperties
        private SelectorMode selectorMode = SelectorMode.getConfiguredValue();
        private boolean selectorWorkaroundTest = Boolean.getBoolean("hazelcast.io.selector.workaround.test");
        private ChannelInitializer channelInitializer;

        public Context() {
            String selectorModeString = SelectorMode.getConfiguredString();
            if (selectorModeString.startsWith(SELECT_NOW_STRING + ",")) {
                idleStrategy = createBackoffIdleStrategy(selectorModeString);
            }
        }

        public Context selectorWorkaroundTest(boolean selectorWorkaroundTest) {
            this.selectorWorkaroundTest = selectorWorkaroundTest;
            return this;
        }

        public Context selectorMode(SelectorMode selectorMode) {
            this.selectorMode = selectorMode;
            return this;
        }

        public Context loggingService(LoggingService loggingService) {
            this.loggingService = loggingService;
            return this;
        }

        public Context metricsRegistry(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
            return this;
        }

        public Context threadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public Context errorHandler(ChannelErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
            return this;
        }

        public Context inputThreadCount(int inputThreadCount) {
            this.inputThreadCount = inputThreadCount;
            return this;
        }

        public Context outputThreadCount(int outputThreadCount) {
            this.outputThreadCount = outputThreadCount;
            return this;
        }

        public Context balancerIntervalSeconds(int balancerIntervalSeconds) {
            this.balancerIntervalSeconds = balancerIntervalSeconds;
            return this;
        }

        public Context channelInitializer(ChannelInitializer channelInitializer) {
            this.channelInitializer = channelInitializer;
            return this;
        }
    }
}

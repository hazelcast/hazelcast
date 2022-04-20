/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_NETWORKING_BYTES_RECEIVED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_NETWORKING_BYTES_SEND;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_NETWORKING_PACKETS_RECEIVED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_NETWORKING_PACKETS_SEND;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_PIPELINEID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_THREAD;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_BALANCER;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION_IN;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION_OUT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_INPUTTHREAD;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_OUTPUTTHREAD;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_NOW_STRING;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_WITH_FIX;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.internal.util.concurrent.BackoffIdleStrategy.createBackoffIdleStrategy;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;

/**
 * A non-blocking {@link Networking} implementation that makes use of
 * {@link java.nio.channels.Selector} to have a limited set of io threads, handle
 * an arbitrary number of connections.
 * <p>
 * Each {@link NioChannel} has 2 parts:
 * <ol>
 * <li>{@link NioInboundPipeline}: triggered by the NioThread when data is available
 * in the socket. The NioInboundPipeline takes care of reading data from the socket
 * and calling the appropriate
 * {@link InboundHandler}</li>
 * <li>{@link NioOutboundPipeline}: triggered by the NioThread when either space
 * is available in the socket for writing, or when there is something that needs to
 * be written e.g. a Packet. The NioOutboundPipeline takes care of calling the
 * appropriate {@link OutboundHandler}
 * to convert the {@link com.hazelcast.internal.networking.OutboundFrame} to bytes
 * in in the ByteBuffer and writing it to the socket.
 * </li>
 * </ol>
 * <p>
 * By default, the {@link NioThread} blocks on the Selector, but it can be put in a
 * 'selectNow' mode that makes it spin on the selector. This is an experimental
 * feature and will cause the io threads to run hot. For this reason, when this feature
 * is enabled, the number of io threads should be reduced (preferably 1).
 */
public final class NioNetworking implements Networking, DynamicMetricsProvider {

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private final AtomicInteger nextOutputThreadIndex = new AtomicInteger();
    private final ILogger logger;
    private final MetricsRegistry metricsRegistry;
    private final LoggingService loggingService;
    private final String threadNamePrefix;
    private final ChannelErrorHandler errorHandler;
    private final int balancerIntervalSeconds;
    private final int inputThreadCount;
    private final int outputThreadCount;
    private final Set<NioChannel> channels = newSetFromMap(new ConcurrentHashMap<>());
    private final ChannelCloseListener channelCloseListener = new ChannelCloseListenerImpl();
    private final SelectorMode selectorMode;
    private final BackoffIdleStrategy idleStrategy;
    private final boolean selectorWorkaroundTest;
    private final boolean selectionKeyWakeupEnabled;
    private final ThreadAffinity outputThreadAffinity;
    private volatile ExecutorService closeListenerExecutor;
    private final ConcurrencyDetection concurrencyDetection;
    private final boolean writeThroughEnabled;
    private final ThreadAffinity inputThreadAffinity;
    private volatile IOBalancer ioBalancer;
    private volatile NioThread[] inputThreads;
    private volatile NioThread[] outputThreads;
    private volatile ScheduledFuture publishFuture;
    // Currently, this is a coarse-grained aggregation of the bytes/send received.
    // In the future you probably want to split this up in member and client and potentially
    // wan specific.
    @Probe(name = NETWORKING_METRIC_NIO_NETWORKING_BYTES_SEND, unit = BYTES, level = DEBUG)
    private volatile long bytesSend;
    @Probe(name = NETWORKING_METRIC_NIO_NETWORKING_BYTES_RECEIVED, unit = BYTES, level = DEBUG)
    private volatile long bytesReceived;
    @Probe(name = NETWORKING_METRIC_NIO_NETWORKING_PACKETS_SEND, level = DEBUG)
    private volatile long packetsSend;
    @Probe(name = NETWORKING_METRIC_NIO_NETWORKING_PACKETS_RECEIVED, level = DEBUG)
    private volatile long packetsReceived;

    public NioNetworking(Context ctx) {
        this.threadNamePrefix = ctx.threadNamePrefix;
        this.metricsRegistry = ctx.metricsRegistry;
        this.loggingService = ctx.loggingService;
        this.inputThreadCount = ctx.inputThreadCount;
        this.outputThreadCount = ctx.outputThreadCount;
        this.logger = loggingService.getLogger(NioNetworking.class);
        this.errorHandler = ctx.errorHandler;
        this.inputThreadAffinity = ctx.inputThreadAffinity;
        this.outputThreadAffinity = ctx.outputThreadAffinity;
        this.balancerIntervalSeconds = ctx.balancerIntervalSeconds;
        this.selectorMode = ctx.selectorMode;
        this.selectorWorkaroundTest = ctx.selectorWorkaroundTest;
        this.idleStrategy = ctx.idleStrategy;
        this.concurrencyDetection = ctx.concurrencyDetection;
        // selector mode SELECT_WITH_FIX requires that a single thread
        // accesses a selector & its selectionKeys. Selection key wake-up
        // and write through break this requirement, therefore must be
        // disabled with SELECT_WITH_FIX.
        this.writeThroughEnabled = ctx.writeThroughEnabled && selectorMode != SELECT_WITH_FIX;
        this.selectionKeyWakeupEnabled = ctx.selectionKeyWakeupEnabled && selectorMode != SELECT_WITH_FIX;
        if (selectorMode == SELECT_WITH_FIX
                && (ctx.writeThroughEnabled || ctx.selectionKeyWakeupEnabled)) {
            logger.warning("Selector mode SELECT_WITH_FIX is incompatible with write-through and selection key wakeup "
                    + "optimizations and they have been disabled. Start Hazelcast with options "
                    + "\"-Dhazelcast.io.selectionKeyWakeupEnabled=false -Dhazelcast.io.write.through=false\" to "
                    + "explicitly disable selection key wakeup and write-through optimizations and avoid logging this "
                    + "warning.");
        }
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NioThread[] getInputThreads() {
        return inputThreads;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public NioThread[] getOutputThreads() {
        return outputThreads;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "used only for testing")
    public Set<NioChannel> getChannels() {
        return channels;
    }

    public IOBalancer getIOBalancer() {
        return ioBalancer;
    }

    @Override
    public void restart() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Can't (re)start an already running NioNetworking");
        }

        if (logger.isFineEnabled()) {
            logger.fine("TcpIpConnectionManager configured with Non Blocking IO-threading model: "
                    + inputThreadCount + " input threads and "
                    + outputThreadCount + " output threads");
            logger.fine("write through enabled:" + writeThroughEnabled);
        }

        logger.log(selectorMode != SELECT ? Level.INFO : FINE, "IO threads selector mode is " + selectorMode);

        publishFuture = metricsRegistry.scheduleAtFixedRate(new PublishAllTask(), 1, SECONDS, ProbeLevel.INFO);

        this.closeListenerExecutor = newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setName(threadNamePrefix + "-NioNetworking-closeListenerExecutor");
            return t;
        });

        NioThread[] inThreads = new NioThread[inputThreadCount];
        for (int i = 0; i < inThreads.length; i++) {
            NioThread thread = new NioThread(
                    createThreadPoolName(threadNamePrefix, "IO") + "in-" + i,
                    loggingService.getLogger(NioThread.class),
                    errorHandler,
                    selectorMode,
                    idleStrategy);
            thread.id = i;
            thread.setSelectorWorkaroundTest(selectorWorkaroundTest);
            thread.setThreadAffinity(inputThreadAffinity);
            inThreads[i] = thread;
            thread.start();
        }
        this.inputThreads = inThreads;

        NioThread[] outThreads = new NioThread[outputThreadCount];
        for (int i = 0; i < outThreads.length; i++) {
            NioThread thread = new NioThread(
                    createThreadPoolName(threadNamePrefix, "IO") + "out-" + i,
                    loggingService.getLogger(NioThread.class),
                    errorHandler,
                    selectorMode,
                    idleStrategy);
            thread.id = i;
            thread.setSelectorWorkaroundTest(selectorWorkaroundTest);
            thread.setThreadAffinity(outputThreadAffinity);
            outThreads[i] = thread;
            thread.start();
        }
        this.outputThreads = outThreads;

        startIOBalancer();

        metricsRegistry.registerDynamicMetricsProvider(this);
    }

    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inputThreads, outputThreads, threadNamePrefix, balancerIntervalSeconds, loggingService);
        ioBalancer.start();
    }

    @Override
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        metricsRegistry.deregisterDynamicMetricsProvider(this);

        // if there are any channels left, we close them.
        for (Channel channel : channels) {
            if (!channel.isClosed()) {
                closeResource(channel);
            }
        }
        //and clear them to prevent memory leaks.
        channels.clear();

        // we unregister the publish future to prevent memory leaks.
        if (publishFuture != null) {
            publishFuture.cancel(false);
            publishFuture = null;
        }
        ioBalancer.stop();

        if (logger.isFinestEnabled()) {
            logger.finest("Shutting down IO Threads... Total: " + (inputThreads.length + outputThreads.length));
        }

        shutdown(inputThreads);
        inputThreads = null;
        shutdown(outputThreads);
        outputThreads = null;
        closeListenerExecutor.shutdown();
        closeListenerExecutor = null;
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
    public Channel register(ChannelInitializer channelInitializer,
                            SocketChannel socketChannel,
                            boolean clientMode) throws IOException {
        if (!started.get()) {
            throw new IllegalArgumentException("Can't register a channel when networking isn't started");
        }

        NioChannel channel = new NioChannel(socketChannel, clientMode, channelInitializer, closeListenerExecutor);

        socketChannel.configureBlocking(false);

        NioInboundPipeline inboundPipeline = newInboundPipeline(channel);
        NioOutboundPipeline outboundPipeline = newOutboundPipeline(channel);
        channel.init(inboundPipeline, outboundPipeline);
        ioBalancer.channelAdded(inboundPipeline, outboundPipeline);
        channel.addCloseListener(channelCloseListener);
        channels.add(channel);
        return channel;
    }

    private NioOutboundPipeline newOutboundPipeline(NioChannel channel) {
        int index = hashToIndex(nextOutputThreadIndex.getAndIncrement(), outputThreadCount);
        NioThread[] threads = outputThreads;
        if (threads == null) {
            throw new IllegalStateException("NioNetworking is shutdown!");
        }

        return new NioOutboundPipeline(
                channel,
                threads[index],
                errorHandler,
                loggingService.getLogger(NioOutboundPipeline.class),
                ioBalancer,
                concurrencyDetection,
                writeThroughEnabled,
                selectionKeyWakeupEnabled);
    }

    private NioInboundPipeline newInboundPipeline(NioChannel channel) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreadCount);
        NioThread[] threads = inputThreads;
        if (threads == null) {
            throw new IllegalStateException("NioNetworking is shutdown!");
        }

        return new NioInboundPipeline(
                channel,
                threads[index],
                errorHandler,
                loggingService.getLogger(NioInboundPipeline.class),
                ioBalancer);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor,
                                      MetricsCollectionContext context) {
        for (Channel channel : channels) {
            String pipelineId = channel.localSocketAddress() + "->" + channel.remoteSocketAddress();

            MetricDescriptor descriptorIn = descriptor
                    .copy()
                    .withPrefix(TCP_PREFIX_CONNECTION_IN)
                    .withDiscriminator(TCP_DISCRIMINATOR_PIPELINEID, pipelineId);
            context.collect(descriptorIn, channel.inboundPipeline());

            MetricDescriptor descriptorOut = descriptor
                    .copy()
                    .withPrefix(TCP_PREFIX_CONNECTION_OUT)
                    .withDiscriminator(TCP_DISCRIMINATOR_PIPELINEID, pipelineId);
            context.collect(descriptorOut, channel.outboundPipeline());
        }

        NioThread[] inputThreads = this.inputThreads;
        if (inputThreads != null) {
            for (NioThread nioThread : inputThreads) {
                MetricDescriptor descriptorInThread = descriptor
                        .copy()
                        .withPrefix(TCP_PREFIX_INPUTTHREAD)
                        .withDiscriminator(TCP_DISCRIMINATOR_THREAD, nioThread.getName());
                context.collect(descriptorInThread, nioThread);
            }
        }

        NioThread[] outputThreads = this.outputThreads;
        if (outputThreads != null) {
            for (NioThread nioThread : outputThreads) {
                MetricDescriptor descriptorOutThread = descriptor
                        .copy()
                        .withPrefix(TCP_PREFIX_OUTPUTTHREAD)
                        .withDiscriminator(TCP_DISCRIMINATOR_THREAD, nioThread.getName());
                context.collect(descriptorOutThread, nioThread);
            }
        }

        IOBalancer ioBalancer = this.ioBalancer;
        if (ioBalancer != null) {
            MetricDescriptor descriptorBalancer = descriptor
                    .copy()
                    .withPrefix(TCP_PREFIX_BALANCER);
            context.collect(descriptorBalancer, ioBalancer);
        }

        MetricDescriptor descriptorTcp = descriptor
                .copy()
                .withPrefix(TCP_PREFIX);
        context.collect(descriptorTcp, this);
    }

    // package private accessors for testing
    boolean isWriteThroughEnabled() {
        return writeThroughEnabled;
    }

    boolean isSelectionKeyWakeupEnabled() {
        return selectionKeyWakeupEnabled;
    }

    private class ChannelCloseListenerImpl implements ChannelCloseListener {
        @Override
        public void onClose(Channel channel) {
            NioChannel nioChannel = (NioChannel) channel;
            channels.remove(channel);
            ioBalancer.channelRemoved(nioChannel.inboundPipeline(), nioChannel.outboundPipeline());
        }
    }

    private class PublishAllTask implements Runnable {

        @Override
        public void run() {
            for (NioChannel channel : channels) {
                NioInboundPipeline inboundPipeline = channel.inboundPipeline;
                NioThread inputThread = inboundPipeline.owner();
                if (inputThread != null) {
                    inputThread.addTaskAndWakeup(inboundPipeline::publishMetrics);
                }

                NioOutboundPipeline outboundPipeline = channel.outboundPipeline;
                NioThread outputThread = outboundPipeline.owner();
                if (outputThread != null) {
                    outputThread.addTaskAndWakeup(outboundPipeline::publishMetrics);
                }
            }

            bytesSend = bytesTransceived(outputThreads);
            bytesReceived = bytesTransceived(inputThreads);
            packetsSend = packetsTransceived(outputThreads);
            packetsReceived = packetsTransceived(inputThreads);
        }

        private long bytesTransceived(NioThread[] threads) {
            if (threads == null) {
                return 0;
            }

            long result = 0;
            for (NioThread nioThread : threads) {
                result += nioThread.bytesTransceived;
            }
            return result;
        }

        private long packetsTransceived(NioThread[] threads) {
            if (threads == null) {
                return 0;
            }

            long result = 0;
            for (NioThread nioThread : threads) {
                result += nioThread.framesTransceived + nioThread.priorityFramesTransceived;
            }
            return result;
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
        private ThreadAffinity inputThreadAffinity = ThreadAffinity.DISABLED;
        private ThreadAffinity outputThreadAffinity = ThreadAffinity.DISABLED;

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
        private boolean selectionKeyWakeupEnabled
                = Boolean.parseBoolean(System.getProperty("hazelcast.io.selectionKeyWakeupEnabled", "true"));
        private ConcurrencyDetection concurrencyDetection;

        // if the calling thread is allowed to write through to the socket if that is possible.
        // this is an optimization that can speed up low threaded setups
        private boolean writeThroughEnabled;

        public Context() {
            String selectorModeString = SelectorMode.getConfiguredString();
            if (selectorModeString.startsWith(SELECT_NOW_STRING + ",")) {
                idleStrategy = createBackoffIdleStrategy(selectorModeString);
            }
        }

        public Context selectionKeyWakeupEnabled(boolean selectionKeyWakeupEnabled) {
            this.selectionKeyWakeupEnabled = selectionKeyWakeupEnabled;
            return this;
        }

        public Context writeThroughEnabled(boolean writeThroughEnabled) {
            this.writeThroughEnabled = writeThroughEnabled;
            return this;
        }

        public Context concurrencyDetection(ConcurrencyDetection concurrencyDetection) {
            this.concurrencyDetection = concurrencyDetection;
            return this;
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
            if (inputThreadAffinity.isEnabled()) {
                return this;
            }
            this.inputThreadCount = inputThreadCount;
            return this;
        }

        public Context outputThreadCount(int outputThreadCount) {
            if (outputThreadAffinity.isEnabled()) {
                return this;
            }
            this.outputThreadCount = outputThreadCount;
            return this;
        }

        public Context inputThreadAffinity(ThreadAffinity inputThreadAffinity) {
            this.inputThreadAffinity = inputThreadAffinity;

            if (inputThreadAffinity.isEnabled()) {
                inputThreadCount = inputThreadAffinity.getThreadCount();
            }
            return this;
        }

        public Context outputThreadAffinity(ThreadAffinity outputThreadAffinity) {
            this.outputThreadAffinity = outputThreadAffinity;

            if (outputThreadAffinity.isEnabled()) {
                outputThreadCount = outputThreadAffinity.getThreadCount();
            }
            return this;
        }

        public Context balancerIntervalSeconds(int balancerIntervalSeconds) {
            this.balancerIntervalSeconds = balancerIntervalSeconds;
            return this;
        }
    }
}

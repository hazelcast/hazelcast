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

package com.hazelcast.internal.networking.spinning;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.io.IOException;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkInstanceOf;

/**
 * A {@link EventLoopGroup} that uses (busy) spinning on the SocketChannels to see if there is something
 * to read or write.
 *
 * Currently there are 2 threads spinning:
 * <ol>
 * <li>1 thread spinning on all SocketChannels for reading</li>
 * <li>1 thread spinning on all SocketChannels for writing</li>
 * </ol>
 * In the future we need to play with this a lot more. 1 thread should be able to saturate a 40GbE connection, but that
 * currently doesn't work for us. So I guess our IO threads are doing too much stuff not relevant like writing the Frames
 * to bytebuffers or converting the bytebuffers to Frames.
 *
 * This is an experimental feature and disabled by default.
 */
public class SpinningEventLoopGroup implements EventLoopGroup {

    private final ChannelCloseListener channelCloseListener = new ChannelCloseListenerImpl();
    private final ILogger logger;
    private final LoggingService loggingService;
    private final SpinningInputThread inputThread;
    private final SpinningOutputThread outputThread;
    private final ChannelInitializer channelInitializer;
    private final ChannelErrorHandler errorHandler;
    private final MetricsRegistry metricsRegistry;
    private final ILogger readerLogger;
    private final ILogger writerLogger;

    public SpinningEventLoopGroup(LoggingService loggingService,
                                  MetricsRegistry metricsRegistry,
                                  ChannelErrorHandler errorHandler,
                                  ChannelInitializer channelInitializer,
                                  String hzName) {
        this.loggingService = loggingService;
        this.logger = loggingService.getLogger(SpinningEventLoopGroup.class);
        this.readerLogger = loggingService.getLogger(SpinningChannelReader.class);
        this.writerLogger = loggingService.getLogger(SpinningChannelReader.class);
        this.metricsRegistry = metricsRegistry;
        this.errorHandler = errorHandler;
        this.inputThread = new SpinningInputThread(hzName);
        this.outputThread = new SpinningOutputThread(hzName);
        this.channelInitializer = channelInitializer;
    }

    @Override
    public void register(final Channel channel) {
        SpinningChannel spinningChannel = checkInstanceOf(SpinningChannel.class, channel);
        try {
            spinningChannel.socketChannel().configureBlocking(false);
        } catch (IOException e) {
            throw rethrow(e);
        }

        SpinningChannelReader reader = new SpinningChannelReader(channel, readerLogger, errorHandler, channelInitializer);
        spinningChannel.setReader(reader);
        inputThread.register(reader);

        SpinningChannelWriter writer = new SpinningChannelWriter(channel, writerLogger, errorHandler, channelInitializer);
        spinningChannel.setWriter(writer);
        outputThread.register(writer);

        String metricsId = channel.getLocalSocketAddress() + "->" + channel.getRemoteSocketAddress();
        metricsRegistry.scanAndRegister(writer, "tcp.connection[" + metricsId + "].out");
        metricsRegistry.scanAndRegister(reader, "tcp.connection[" + metricsId + "].in");

        channel.addCloseListener(channelCloseListener);
    }

    @Override
    public void start() {
        logger.info("TcpIpConnectionManager configured with Spinning IO-threading model: "
                + "1 input thread and 1 output thread");
        inputThread.start();
        outputThread.start();
    }

    @Override
    public void shutdown() {
        inputThread.shutdown();
        outputThread.shutdown();
    }

    private class ChannelCloseListenerImpl implements ChannelCloseListener {
        @Override
        public void onClose(Channel channel) {
            SpinningChannel spinningChannel = (SpinningChannel) channel;

            metricsRegistry.deregister(spinningChannel.getReader());
            metricsRegistry.deregister(spinningChannel.getWriter());

            outputThread.unregister(spinningChannel.getWriter());
            inputThread.unregister(spinningChannel.getReader());
        }
    }
}

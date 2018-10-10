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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.AbstractChannel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.OutboundFrame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * A {@link com.hazelcast.internal.networking.Channel} implementation tailored
 * for non blocking IO using {@link java.nio.channels.Selector} in combination
 * with a non blocking {@link SocketChannel}.
 */
public final class NioChannel extends AbstractChannel {

    private static final int DELAY_MS = Integer.getInteger("hazelcast.channel.close.delayMs", 200);
    NioInboundPipeline inboundPipeline;
    NioOutboundPipeline outboundPipeline;

    private final MetricsRegistry metricsRegistry;
    private final ChannelInitializer channelInitializer;
    private final NioChannelOptions config;

    public NioChannel(SocketChannel socketChannel,
                      boolean clientMode,
                      ChannelInitializer channelInitializer,
                      MetricsRegistry metricsRegistry) {
        super(socketChannel, clientMode);
        this.channelInitializer = channelInitializer;
        this.metricsRegistry = metricsRegistry;
        this.config = new NioChannelOptions(socketChannel.socket());
    }

    @Override
    public NioChannelOptions options() {
        return config;
    }

    public void init(NioInboundPipeline inboundPipeline, NioOutboundPipeline outboundPipeline) {
        this.inboundPipeline = inboundPipeline;
        this.outboundPipeline = outboundPipeline;
    }

    public NioOutboundPipeline outboundPipeline() {
        return outboundPipeline;
    }

    public NioInboundPipeline inboundPipeline() {
        return inboundPipeline;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (isClosed()) {
            return false;
        }
        outboundPipeline.write(frame);
        return true;
    }

    @Override
    protected void onConnect() {
        String metricsId = localSocketAddress() + "->" + remoteSocketAddress();
        metricsRegistry.scanAndRegister(outboundPipeline, "tcp.connection[" + metricsId + "].out");
        metricsRegistry.scanAndRegister(inboundPipeline, "tcp.connection[" + metricsId + "].in");
    }

    @Override
    public long lastReadTimeMillis() {
        return inboundPipeline.lastReadTimeMillis();
    }

    @Override
    public long lastWriteTimeMillis() {
        return outboundPipeline.lastWriteTimeMillis();
    }

    @Override
    public void start() {
        try {
            // before starting the channel, the socketChannel need to be put in
            // non blocking mode since that is mandatory for the NioChannel.
            socketChannel.configureBlocking(false);
            channelInitializer.initChannel(this);
        } catch (Exception e) {
            throw new HazelcastException("Failed to start " + this, e);
        }
        inboundPipeline.start();
        outboundPipeline.start();
    }

    @Override
    protected void close0() {
        if (Thread.currentThread() instanceof NioThread) {
            new Thread() {
                public void run() {
                    try {
                        doClose();
                    } catch (Exception e) {
                        logger.warning(e.getMessage(), e);
                    }
                }
            }.start();
        } else {
            doClose();
        }
    }

    private void doClose() {
        try {
            inboundPipeline.requestClose();
            outboundPipeline.requestClose();

            if (DELAY_MS > 0) {
                try {
                    Thread.sleep(DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            try {
                socketChannel.close();
            } catch (IOException e) {
                logger.warning(e);
            }
        } finally {
            notifyCloseListeners();
        }
    }

    @Override
    public String toString() {
        return "NioChannel{" + localSocketAddress() + "->" + remoteSocketAddress() + '}';
    }

      //  this toString implementation is very useful for debugging. Please don't remove it.
//    @Override
//    public String toString() {
//        String local = getPort(localSocketAddress());it
//        String remote = getPort(remoteSocketAddress());
//        String s = local + (isClientMode() ? "=>" : "->") + remote;
//
//        // this is added for debugging so that 'client' and 'server' have a different indentation and are easy to recognize.
//        if (!isClientMode()) {
//            s = "                                                                                " + s;
//        }
//
//        Date date = new Date();
//        return date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds() + " " + s;
//    }

    private String getPort(SocketAddress socketAddress) {
        return socketAddress == null ? "*missing*" : Integer.toString(((InetSocketAddress) socketAddress).getPort());
    }
}

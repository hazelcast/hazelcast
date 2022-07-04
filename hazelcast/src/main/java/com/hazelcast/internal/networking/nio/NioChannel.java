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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.OutboundFrame;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * A {@link Channel} implementation tailored for non-blocking IO using
 * {@link Selector} in combination with a non-blocking {@link SocketChannel}.
 */
public final class NioChannel extends AbstractChannel {

    // The close delays is needed for the TLS goodbye handshake to complete.
    NioInboundPipeline inboundPipeline;
    NioOutboundPipeline outboundPipeline;

    private final Executor closeListenerExecutor;
    private final ChannelInitializer channelInitializer;
    private final NioChannelOptions config;

    public NioChannel(SocketChannel socketChannel,
                      boolean clientMode,
                      ChannelInitializer channelInitializer,
                      Executor closeListenerExecutor) {
        super(socketChannel, clientMode);
        this.channelInitializer = channelInitializer;
        this.closeListenerExecutor = closeListenerExecutor;
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
            // non-blocking mode since that is mandatory for the NioChannel.
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
        outboundPipeline.drainWriteQueues();

        // the socket is immediately closed.
        try {
            socketChannel.close();
        } catch (IOException e) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed to close " + this, e);
            }
        }

        if (Thread.currentThread() instanceof NioThread) {
            // we don't want to do any tasks on an io thread; we offload it instead
            try {
                closeListenerExecutor.execute(() -> {
                    try {
                        notifyCloseListeners();
                    } catch (Exception e) {
                        logger.warning(e.getMessage(), e);
                    }
                });
            } catch (RejectedExecutionException e) {
                // if the task gets rejected, the networking must be shutting down.
                logger.fine(e);
            }
        } else {
            notifyCloseListeners();
        }
    }

    @Override
    public long bytesRead() {
        return inboundPipeline.bytesRead();
    }

    @Override
    public long bytesWritten() {
        return outboundPipeline.bytesWritten();
    }

    @Override
    public String toString() {
        return "NioChannel{" + localSocketAddress() + "->" + remoteSocketAddress() + '}';
    }

    //  this toString implementation is very useful for debugging. Please don't remove it.
//    @Override
//    public String toString() {
//        String local = getPort(localSocketAddress());
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

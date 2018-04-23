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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * When the {@link NioThread} receives a read event from the {@link java.nio.channels.Selector}, then the
 * {@link #process()} is called to read out the data from the socket into a bytebuffer and hand it over to the
 * {@link ChannelInboundHandler} to get processed.
 */
public final class NioInboundPipeline extends NioPipeline {

    protected ByteBuffer inputBuffer;

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private final ChannelInitializer initializer;
    private ChannelInboundHandler inboundHandler;
    private volatile long lastReadTime;

    private volatile long bytesReadLastPublish;
    private volatile long normalFramesReadLastPublish;
    private volatile long priorityFramesReadLastPublish;
    private volatile long processCountLastPublish;

    public NioInboundPipeline(
            NioChannel channel,
            NioThread owner,
            ChannelErrorHandler errorHandler,
            ILogger logger,
            IOBalancer balancer,
            ChannelInitializer initializer) {
        super(channel, owner, errorHandler, OP_READ, logger, balancer);
        this.initializer = initializer;
    }

    @Override
    public long load() {
        switch (loadType) {
            case LOAD_BALANCING_HANDLE:
                return processCount.get();
            case LOAD_BALANCING_BYTE:
                return bytesRead.get();
            case LOAD_BALANCING_FRAME:
                return normalFramesRead.get() + priorityFramesRead.get();
            default:
                throw new RuntimeException();
        }
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastReadTime, 0);
    }

    public SwCounter getNormalFramesReadCounter() {
        return normalFramesRead;
    }

    public SwCounter getPriorityFramesReadCounter() {
        return priorityFramesRead;
    }

    public long lastReadTimeMillis() {
        return lastReadTime;
    }

    @Override
    void process() throws Exception {
        processCount.inc();
        // we are going to set the timestamp even if the channel is going to fail reading. In that case
        // the connection is going to be closed anyway.
        lastReadTime = currentTimeMillis();

        if (inboundHandler == null && !init()) {
            return;
        }

        int readBytes = channel.read(inputBuffer);
        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }

        bytesRead.inc(readBytes);

        inputBuffer.flip();
        inboundHandler.onRead(inputBuffer);
        compactOrClear(inputBuffer);
    }

    private boolean init() throws IOException {
        InitResult<ChannelInboundHandler> init = initializer.initInbound(channel);
        if (init == null) {
            // we can't initialize yet
            return false;
        }
        this.inboundHandler = init.getHandler();
        this.inputBuffer = init.getByteBuffer();

        if (inboundHandler instanceof ChannelInboundHandlerWithCounters) {
            ChannelInboundHandlerWithCounters withCounters = (ChannelInboundHandlerWithCounters) inboundHandler;
            withCounters.setNormalPacketsRead(normalFramesRead);
            withCounters.setPriorityPacketsRead(priorityFramesRead);
        }

        return true;
    }

    @Override
    void publishMetrics() {
        if (Thread.currentThread() != owner) {
            return;
        }

        // since this is executed by the owner, the owner field can't change while
        // this method is executed.
        owner.bytesTransceived += bytesRead.get() - bytesReadLastPublish;
        owner.framesTransceived += normalFramesRead.get() - normalFramesReadLastPublish;
        owner.priorityFramesTransceived += priorityFramesRead.get() - priorityFramesReadLastPublish;
        owner.processCount += processCount.get() - processCountLastPublish;

        bytesReadLastPublish = bytesRead.get();
        normalFramesReadLastPublish = normalFramesRead.get();
        priorityFramesReadLastPublish = priorityFramesRead.get();
        processCountLastPublish = processCount.get();
    }

    @Override
    public void close() {
        addTaskAndWakeup(new NioPipelineTask(this) {
            @Override
            public void run0() {
                try {
                    channel.closeInbound();
                } catch (IOException e) {
                    logger.finest("Error while closing inbound", e);
                }
            }
        });
    }

    @Override
    public String toString() {
        return channel + ".inboundPipeline";
    }

}

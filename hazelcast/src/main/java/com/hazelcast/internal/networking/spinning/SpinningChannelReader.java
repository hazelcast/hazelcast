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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.networking.nio.ChannelInboundHandlerWithCounters;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class SpinningChannelReader extends AbstractHandler {

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private final ChannelInitializer initializer;
    private volatile long lastReadTime;
    private ChannelInboundHandler inboundHandler;
    private ByteBuffer inputBuffer;

    public SpinningChannelReader(Channel channel,
                                 ILogger logger,
                                 ChannelErrorHandler errorHandler,
                                 ChannelInitializer initializer) {
        super(channel, logger, errorHandler);
        this.initializer = initializer;
    }

    public long lastReadTimeMillis() {
        return lastReadTime;
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastReadTime, 0);
    }

    public SwCounter getNormalFramesReadCounter() {
        return normalFramesRead;
    }

    public SwCounter getPriorityFramesReadCounter() {
        return priorityFramesRead;
    }

    public void read() throws Exception {
        if (channel.isClosed()) {
            channel.closeInbound();
            return;
        }

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

        lastReadTime = currentTimeMillis();
        bytesRead.inc(readBytes);
        inputBuffer.flip();
        inboundHandler.onRead(inputBuffer);
        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
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
}

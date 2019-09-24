/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.networking.ChannelHandler;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.InboundPipeline;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.Arrays;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.collection.ArrayUtils.append;
import static com.hazelcast.internal.util.collection.ArrayUtils.replaceFirst;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * When the {@link NioThread} receives a read event from the
 * {@link Selector}, then the {@link #process()} is called to read
 * out the data from the socket into a bytebuffer and hand it over to the
 * {@link InboundHandler} to get processed.
 */
public final class NioInboundPipeline extends NioPipeline implements InboundPipeline {

    private InboundHandler[] handlers = new InboundHandler[0];
    private ByteBuffer receiveBuffer;

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private volatile long lastReadTime;

    private volatile long bytesReadLastPublish;
    private volatile long normalFramesReadLastPublish;
    private volatile long priorityFramesReadLastPublish;
    private volatile long processCountLastPublish;

    NioInboundPipeline(NioChannel channel,
                       NioThread owner,
                       ChannelErrorHandler errorHandler,
                       ILogger logger,
                       IOBalancer balancer) {
        super(channel, owner, errorHandler, OP_READ, logger, balancer);
    }

    public long normalFramesRead() {
        return normalFramesRead.get();
    }

    public long priorityFramesRead() {
        return priorityFramesRead.get();
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

    public long lastReadTimeMillis() {
        return lastReadTime;
    }

    @Override
    void process() throws Exception {
        processCount.inc();
        // we are going to set the timestamp even if the channel is going to fail reading. In that case
        // the connection is going to be closed anyway.
        lastReadTime = currentTimeMillis();

        int readBytes = socketChannel.read(receiveBuffer);

        if (readBytes == -1) {
            throw new EOFException("Remote socket closed!");
        }

        bytesRead.inc(readBytes);

        // currently the whole pipeline is retried when one of the handlers is dirty; but only the dirty handler
        // and the remaining sequence should need to retry.
        InboundHandler[] localHandlers = handlers;
        boolean cleanPipeline;
        boolean unregisterRead;
        do {
            cleanPipeline = true;
            unregisterRead = false;
            for (int handlerIndex = 0; handlerIndex < localHandlers.length; handlerIndex++) {
                InboundHandler handler = localHandlers[handlerIndex];
                HandlerStatus handlerStatus = handler.onRead();
                if (localHandlers != handlers) {
                    // change in the pipeline detected, restarting loop
                    handlerIndex = -1;
                    localHandlers = handlers;
                    continue;
                }

                switch (handlerStatus) {
                    case CLEAN:
                        break;
                    case DIRTY:
                        cleanPipeline = false;
                        break;
                    case BLOCKED:
                        // setting cleanPipeline to true keep flushing everything downstream, but not upstream.
                        cleanPipeline = true;
                        unregisterRead = true;
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        } while (!cleanPipeline);

        if (migrationRequested()) {
            startMigration();
            return;
        }

        if (unregisterRead) {
            unregisterOp(OP_READ);
        }
    }

    @Override
    void publishMetrics() {
        if (currentThread() != owner) {
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
    public String toString() {
        return channel + ".inboundPipeline";
    }

    @Override
    protected Iterable<? extends ChannelHandler> handlers() {
        return Arrays.asList(handlers);
    }

    @Override
    public InboundPipeline remove(InboundHandler handler) {
        return replace(handler);
    }

    @Override
    public InboundPipeline addLast(InboundHandler... addedHandlers) {
        checkNotNull(addedHandlers, "handlers can't be null");

        for (InboundHandler addedHandler : addedHandlers) {
            fixDependencies(addedHandler);
            addedHandler.setChannel(channel).handlerAdded();
        }

        updatePipeline(append(handlers, addedHandlers));
        return this;
    }

    @Override
    public InboundPipeline replace(InboundHandler oldHandler, InboundHandler... addedHandlers) {
        checkNotNull(oldHandler, "oldHandler can't be null");
        checkNotNull(addedHandlers, "addedHandlers can't be null");

        InboundHandler[] newHandlers = replaceFirst(handlers, oldHandler, addedHandlers);
        if (newHandlers == handlers) {
            throw new IllegalArgumentException("handler " + oldHandler + " isn't part of the pipeline");
        }

        for (InboundHandler addedHandler : addedHandlers) {
            fixDependencies(addedHandler);
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(newHandlers);
        return this;
    }

    private void fixDependencies(ChannelHandler addedHandler) {
        if (addedHandler instanceof InboundHandlerWithCounters) {
            InboundHandlerWithCounters c = (InboundHandlerWithCounters) addedHandler;
            c.setNormalPacketsRead(normalFramesRead);
            c.setPriorityPacketsRead(priorityFramesRead);
        }
    }

    private void updatePipeline(InboundHandler[] handlers) {
        this.handlers = handlers;
        receiveBuffer = handlers.length == 0 ? null : (ByteBuffer) handlers[0].src();

        InboundHandler prev = null;
        for (InboundHandler handler : handlers) {
            if (prev != null) {
                Object src = handler.src();
                if (src instanceof ByteBuffer) {
                    prev.dst(src);
                }
            }
            prev = handler;
        }
    }

    // useful for debugging
    private String pipelineToString() {
        StringBuilder sb = new StringBuilder("in-pipeline[");
        InboundHandler[] handlers = this.handlers;
        for (int k = 0; k < handlers.length; k++) {
            if (k > 0) {
                sb.append("->-");
            }
            sb.append(handlers[k].getClass().getSimpleName());
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    public NioInboundPipeline wakeup() {
        ownerAddTaskAndWakeup(new NioPipelineTask(this) {
            @Override
            protected void run0() throws IOException {
                registerOp(OP_READ);
                NioInboundPipeline.this.run();
            }
        });

        return this;
    }
}

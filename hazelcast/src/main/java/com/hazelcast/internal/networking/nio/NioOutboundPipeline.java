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
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.OutboundPipeline;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.collection.ArrayUtils.append;
import static com.hazelcast.util.collection.ArrayUtils.replaceFirst;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_WRITE;

public final class NioOutboundPipeline
        extends NioPipeline
        implements OutboundPipeline {

    private OutboundHandler[] handlers = new OutboundHandler[0];
    private ByteBuffer sendBuffer;

    private final ConcurrencyDetection concurrencyDetection;
    private final boolean writeThroughEnabled;

    //private final AtomicBoolean scheduled = new AtomicBoolean(false);
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();

    private final SwCounter lastWriteTime = newSwCounter();

    private long bytesWrittenLastPublish;
    private long normalFramesWrittenLastPublish;
    private long priorityFramesWrittenLastPublish;
    private long processCountLastPublish;
    private SendQueue sendQueue = new SendQueue(bytesWritten);

    NioOutboundPipeline(NioChannel channel,
                        NioThread owner,
                        ChannelErrorHandler errorHandler,
                        ILogger logger,
                        IOBalancer balancer,
                        ConcurrencyDetection concurrencyDetection,
                        boolean writeThroughEnabled) {
        super(channel, owner, errorHandler, OP_WRITE, logger, balancer);
        this.concurrencyDetection = concurrencyDetection;
        this.writeThroughEnabled = writeThroughEnabled;
    }

    @Override
    public long load() {
        switch (loadType) {
            case LOAD_BALANCING_HANDLE:
                return processCount.get();
            case LOAD_BALANCING_BYTE:
                return bytesWritten.get();
            case LOAD_BALANCING_FRAME:
                return normalFramesWritten.get() + priorityFramesWritten.get();
            default:
                throw new RuntimeException();
        }
    }

    public int totalFramesPending() {
        return sendQueue.totalFramesPending();
    }

    public long lastWriteTimeMillis() {
        return lastWriteTime.get();
    }

    @Probe(name = "writeQueuePendingBytes", level = DEBUG)
    public long bytesPending() {
        return sendQueue.bytesPending();
    }

    @Probe(name = "priorityWriteQueuePendingBytes", level = DEBUG)
    public long priorityBytesPending() {
        return sendQueue.priorityBytesPending();
    }

    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastWriteTime.get(), 0);
    }

    @Probe(level = DEBUG)
    private long isScheduled() {
        return sendQueue.state().ordinal();
    }

    @Override
    public void execute(Runnable task) {
        if (sendQueue.execute(task)) {
            owner.addTaskAndWakeup(this);
        }
    }

    public void write(OutboundFrame frame) {
        // todo: in the old implementation we could detect concurrency by checking if the pipeline was already
        // scheduled. But currently we have lost that ability.

        if (sendQueue.offer(frame)) {
            // we manage to schedule the pipeline, meaning we are owner.
            if (writeThroughEnabled && !concurrencyDetection.isDetected()) {
                // we are allowed to do a write through, so lets process the request on the calling thread
                try {
                    process();
                } catch (Throwable t) {
                    onError(t);
                }
            } else {
                // no write through, let the io thread deal with it.
                owner.addTaskAndWakeup(this);
            }
        }
    }

    @Override
    public OutboundPipeline wakeup() {
        execute(() -> {
        });
        return this;
    }

    @Override
    void start() {
        execute(() -> {
            try {
                getSelectionKey();
            } catch (Throwable t) {
                onError(t);
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process() throws Exception {
        processCount.inc();

        sendQueue.prepare();

        OutboundHandler[] localHandlers = handlers;
        HandlerStatus pipelineStatus = CLEAN;
        for (int handlerIndex = 0; handlerIndex < localHandlers.length; handlerIndex++) {
            OutboundHandler handler = localHandlers[handlerIndex];

            HandlerStatus handlerStatus = handler.onWrite();

            if (localHandlers != handlers) {
                // change in the pipeline detected, therefor the pipeline is restarted.
                localHandlers = handlers;
                pipelineStatus = CLEAN;
                handlerIndex = -1;
            } else if (handlerStatus != CLEAN) {
                pipelineStatus = handlerStatus;
            }
        }

        writeToSocket();

        if (migrationReguested) {
            startMigration();
            return;
        }

        if (sendBuffer.remaining() > 0) {
            pipelineStatus = DIRTY;
        }

        switch (pipelineStatus) {
            case CLEAN:
                unregisterOp(OP_WRITE);
                if (sendQueue.tryUnschedule()) {
                    // the pipeline is dirty; so we need to reschedule this pipeline
                    if (Thread.currentThread().getClass() == NioThread.class) {
                        // we are on the IO thread; we don't need to notify the IO thread
                        owner().addTask(this);
                    } else {
                        // we are not on the IO thread, so we need to to notify it.
                        owner().addTaskAndWakeup(this);
                    }
                }
                break;
            case DIRTY:
                // pipeline is dirty, so lets register for an OP_WRITE to write more data.
                registerOp(OP_WRITE);

                if (writeThroughEnabled && !(Thread.currentThread() instanceof NioThread)) {
                    // there was a write through. Changing the interested set of the selection key
                    // after the IO thread did a select, will not lead to the selector waking up. So
                    // if we don't wake up the selector explicitly, only after the selector.select(timeout)
                    // has expired the selectionKey will be seen. For more info see:
                    // https://stackoverflow.com/questions/11523471/java-selectionkey-interestopsint-not-thread-safe
                    owner.getSelector().wakeup();
                    concurrencyDetection.onDetected();
                }
                break;
            case BLOCKED:
                unregisterOp(OP_WRITE);
                if (sendQueue.block()) {
                    owner.addTaskAndWakeup(this);
                }
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void writeToSocket() throws IOException {
        lastWriteTime.set(currentTimeMillis());
        int written = socketChannel.write(sendBuffer);
        bytesWritten.inc(written);
        //System.out.println(this + " bytes written:" + written + " sendBuffer.capacity:" + sendBuffer.capacity());
    }

    void drainWriteQueues() {
        // putStack.clear();
        // priorityWriteQueue.clear();
        //putStack.set(null);
    }

    @Override
    protected void publishMetrics() {
        if (currentThread() != owner) {
            return;
        }

        owner.bytesTransceived += bytesWritten.get() - bytesWrittenLastPublish;
        owner.framesTransceived += normalFramesWritten.get() - normalFramesWrittenLastPublish;
        owner.priorityFramesTransceived += priorityFramesWritten.get() - priorityFramesWrittenLastPublish;
        owner.processCount += processCount.get() - processCountLastPublish;

        bytesWrittenLastPublish = bytesWritten.get();
        normalFramesWrittenLastPublish = normalFramesWritten.get();
        priorityFramesWrittenLastPublish = priorityFramesWritten.get();
        processCountLastPublish = processCount.get();
    }

    @Override
    public String toString() {
        return channel + ".outboundPipeline";
    }

    @Override
    protected Iterable<? extends ChannelHandler> handlers() {
        return Arrays.asList(handlers);
    }

    @Override
    public OutboundPipeline remove(OutboundHandler handler) {
        return replace(handler);
    }

    @Override
    public OutboundPipeline addLast(OutboundHandler... addedHandlers) {
        checkNotNull(addedHandlers, "addedHandlers can't be null");

        for (OutboundHandler addedHandler : addedHandlers) {
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(append(handlers, addedHandlers));
        return this;
    }

    @Override
    public OutboundPipeline replace(OutboundHandler oldHandler, OutboundHandler... addedHandlers) {
        checkNotNull(oldHandler, "oldHandler can't be null");
        checkNotNull(addedHandlers, "newHandler can't be null");

        OutboundHandler[] newHandlers = replaceFirst(handlers, oldHandler, addedHandlers);
        if (newHandlers == handlers) {
            throw new IllegalArgumentException("handler " + oldHandler + " isn't part of the pipeline");
        }

        for (OutboundHandler addedHandler : addedHandlers) {
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(newHandlers);
        return this;
    }

    private void updatePipeline(OutboundHandler[] newHandlers) {
        this.handlers = newHandlers;
        this.sendBuffer = newHandlers.length == 0 ? null : (ByteBuffer) newHandlers[newHandlers.length - 1].dst();

        OutboundHandler prev = null;
        for (OutboundHandler handler : handlers) {
            if (prev == null) {
                handler.src(sendQueue);
            } else {
                Object src = prev.dst();
                if (src instanceof ByteBuffer) {
                    handler.src(src);
                }
            }
            prev = handler;
        }
    }

    // useful for debugging
    private String pipelineToString() {
        StringBuilder sb = new StringBuilder("out-pipeline[");
        OutboundHandler[] handlers = this.handlers;
        for (int k = 0; k < handlers.length; k++) {
            if (k > 0) {
                sb.append("->-");
            }
            sb.append(handlers[k].getClass().getSimpleName());
        }
        sb.append(']');
        return sb.toString();
    }
}

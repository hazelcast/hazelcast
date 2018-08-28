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
import com.hazelcast.internal.networking.ChannelHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.OutboundPipeline;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.function.Supplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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
        implements Supplier<OutboundFrame>, OutboundPipeline {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<OutboundFrame>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> priorityWriteQueue = new ConcurrentLinkedQueue<OutboundFrame>();

    private OutboundHandler[] handlers = new OutboundHandler[0];
    private ByteBuffer sendBuffer;

    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();

    private volatile long lastWriteTime;

    private long bytesWrittenLastPublish;
    private long normalFramesWrittenLastPublish;
    private long priorityFramesWrittenLastPublish;
    private long processCountLastPublish;

    NioOutboundPipeline(NioChannel channel,
                        NioThread owner,
                        ChannelErrorHandler errorHandler,
                        ILogger logger,
                        IOBalancer balancer) {
        super(channel, owner, errorHandler, OP_WRITE, logger, balancer);
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
        return writeQueue.size() + priorityWriteQueue.size();
    }

    public long lastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Probe(name = "writeQueuePendingBytes", level = DEBUG)
    public long bytesPending() {
        return bytesPending(writeQueue);
    }

    @Probe(name = "priorityWriteQueuePendingBytes", level = DEBUG)
    public long priorityBytesPending() {
        return bytesPending(priorityWriteQueue);
    }

    private long bytesPending(Queue<OutboundFrame> writeQueue) {
        long bytesPending = 0;
        for (OutboundFrame frame : writeQueue) {
            bytesPending += frame.getFrameLength();
        }
        return bytesPending;
    }

    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Probe(level = DEBUG)
    private long isScheduled() {
        return scheduled.get() ? 1 : 0;
    }

    public void write(OutboundFrame frame) {
        if (frame.isUrgent()) {
            priorityWriteQueue.offer(frame);
        } else {
            writeQueue.offer(frame);
        }
        schedule();
    }

    @Override
    public OutboundFrame get() {
        OutboundFrame frame = priorityWriteQueue.poll();
        if (frame == null) {
            frame = writeQueue.poll();
            if (frame == null) {
                return null;
            }
            normalFramesWritten.inc();
        } else {
            priorityFramesWritten.inc();
        }

        return frame;
    }

    /**
     * Makes sure this OutboundHandler is scheduled to be executed by the IO thread.
     * <p/>
     * This call is made by 'outside' threads that interact with the connection. For example when a frame is placed
     * on the connection to be written. It will never be made by an IO thread.
     * <p/>
     * If the OutboundHandler already is scheduled, the call is ignored.
     */
    private void schedule() {
        if (scheduled.get()) {
            // So this pipeline is still scheduled, we don't need to schedule it again
            return;
        }

        if (!scheduled.compareAndSet(false, true)) {
            // Another thread already has scheduled this pipeline, we are done. It
            // doesn't matter which thread does the scheduling, as long as it happens.
            return;
        }

        addTaskAndWakeup(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process() throws Exception {
        processCount.inc();

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

        flushToSocket();

        if (sendBuffer.remaining() > 0) {
            pipelineStatus = DIRTY;
        }

        switch (pipelineStatus) {
            case CLEAN:
                // There is nothing left to be done; so lets unschedule this pipeline
                unschedule();
                break;
            case DIRTY:
                // pipeline is dirty, so lets register for an OP_WRITE to write
                // more data.
                registerOp(OP_WRITE);
                break;
            case BLOCKED:
                // pipeline is blocked; no point in receiving OP_WRITE events.
                unregisterOp(OP_WRITE);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Tries to unschedule this pipeline.
     * <p/>
     * It will only be unscheduled if:
     * - there are no pending frames.
     * <p/>
     * If the outputBuffer is dirty then it will register itself for an OP_WRITE since we are interested in knowing
     * if there is more space in the socket output buffer.
     * If the outputBuffer is not dirty, then it will unregister itself from an OP_WRITE since it isn't interested
     * in space in the socket outputBuffer.
     * <p/>
     * This call is only made by the owning IO thread.
     */
    private void unschedule() throws IOException {
        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(OP_WRITE);
        // So the outputBuffer is empty, so we are going to unschedule the pipeline.
        scheduled.set(false);

        if (writeQueue.isEmpty() && priorityWriteQueue.isEmpty()) {
            // there are no remaining frames, so we are done.
            return;
        }

        // So there are frames, but we just unscheduled ourselves. If we don't try to reschedule, then these
        // Frames are at risk not to be send.
        if (!scheduled.compareAndSet(false, true)) {
            //someone else managed to schedule this OutboundHandler, so we are done.
            return;
        }

        // We managed to reschedule. So lets add ourselves to the owner so we are processed again.
        // We don't need to call wakeup because the current thread is the IO-thread and the selectionQueue will be processed
        // till it is empty. So it will also pick up tasks that are added while it is processing the selectionQueue.

        // owner can't be null because this method is made by the owning io thread.
        owner().addTask(this);
    }

    private void flushToSocket() throws IOException {
        lastWriteTime = currentTimeMillis();
        int written = socketChannel.write(sendBuffer);
        bytesWritten.inc(written);
        //System.out.println(channel+" bytes written:"+written);
    }

    @Override
    public void requestClose() {
        writeQueue.clear();
        priorityWriteQueue.clear();
        super.requestClose();
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
                handler.src(this);
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

    @Override
    public OutboundPipeline wakeup() {
        addTaskAndWakeup(this);
        return this;
    }
}

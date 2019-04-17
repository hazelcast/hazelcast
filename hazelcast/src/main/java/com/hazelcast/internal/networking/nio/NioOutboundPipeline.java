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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

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

    public static class WriteNode {
        WriteNode next;
        OutboundFrame frame;
        Runnable task;

        public WriteNode(OutboundFrame frame) {
            this.frame = frame;
            this.task = null;
        }

        public WriteNode(Runnable task) {
            this.task = task;
            this.frame = null;
        }

        boolean isCleared() {
            return frame == null && task == null;
        }

        @Override
        public String toString() {
            return "WriteNode{" +
                    "next=" + next +
                    ", frame=" + frame +
                    ", task=" + task +
                    ", hash=" + hashCode() +
                    '}';
        }
    }

    public final AtomicReference<WriteNode> writeQueue = new AtomicReference<>();
    // @SuppressWarnings("checkstyle:visibilitymodifier")
    // @Probe(name = "priorityWriteQueueSize")
    // public final Queue<OutboundFrame> priorityWriteQueue = new ConcurrentLinkedQueue<OutboundFrame>();

    private OutboundHandler[] handlers = new OutboundHandler[0];
    private ByteBuffer sendBuffer;

    //private final AtomicBoolean scheduled = new AtomicBoolean(false);
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
//
//    private final static WriteNode SCHEDULED = new WriteNode((Runnable) null) {
//        @Override
//        public String toString() {
//            return "SCHEDULED";
//        }
//    };

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
        return 0;//return writeQueue.size() + priorityWriteQueue.size();
    }

    public long lastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Probe(name = "writeQueuePendingBytes", level = DEBUG)
    public long bytesPending() {
        return 0;//return bytesPending(writeQueue);
    }

    @Probe(name = "priorityWriteQueuePendingBytes", level = DEBUG)
    public long priorityBytesPending() {
        //return bytesPending(priorityWriteQueue);
        return 0;
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
        return writeQueue.get() == null ? 0 : 1;
    }

    @Override
    public void execute(Runnable task) {
        schedule(new WriteNode(task));
    }

    public void write(OutboundFrame frame) {
        schedule(new WriteNode(frame));
    }

    private void schedule(WriteNode update) {
        for (; ; ) {
            WriteNode old = writeQueue.get();
            update.next = old;
            if (writeQueue.compareAndSet(old, update)) {
                if (old == null) {
                    owner.addTaskAndWakeup(this);
                }
                return;
            }
        }
    }

    private WriteNode head;

    @Override
    public OutboundFrame get() {
        for (; ; ) {
            if (head == null) {
                head = writeQueue.get();
                if (head == null) {
                    // there is no work, we are done
                    return null;
                }
            }

            if (head.task != null) {
                Runnable task = head.task;
                head.task = null;
                head = head.next;
                task.run();
            } else if (head.frame != null) {
                OutboundFrame frame = head.frame;
                head.frame = null;
                head = head.next;
                normalFramesWritten.inc();
                return frame;
            } else {
                head = null;
                return null;
            }
        }
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
                // There is nothing left to be done; so lets unschedule this pipeline

                // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
                unregisterOp(OP_WRITE);

                WriteNode node = writeQueue.get();
                if (node.isCleared() && writeQueue.compareAndSet(node, null)) {
                    // writeQueue is clean, so we are done. Now somebody else his concern to schedule.
                    return;
                }

                // writeQueue is dirty, so lets reprocess the pipeline.
                owner.addTask(this);
                break;
            case DIRTY:
                // pipeline is dirty, so lets register for an OP_WRITE to write more data.
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

    private void writeToSocket() throws IOException {
        lastWriteTime = currentTimeMillis();
        int written = socketChannel.write(sendBuffer);
        bytesWritten.inc(written);
        //System.out.println(channel+" bytes written:"+written);
    }

    void drainWriteQueues() {
        // writeQueue.clear();
        // priorityWriteQueue.clear();
        //writeQueue.set(null);
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
        if (writeQueue.compareAndSet(null, new WriteNode(this))) {
            owner.addTaskAndWakeup(this);
        }

        //addTaskAndWakeup(this);
        return this;
    }
}

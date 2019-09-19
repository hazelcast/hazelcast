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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.collection.ArrayUtils.append;
import static com.hazelcast.internal.util.collection.ArrayUtils.replaceFirst;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_WRITE;

public final class NioOutboundPipeline
        extends NioPipeline
        implements Supplier<OutboundFrame>, OutboundPipeline {

    public enum State {
        /*
         * The pipeline isn't scheduled (nothing to do).
         * Only possible next state is scheduled.
         */
        UNSCHEDULED,
        /*
         * The pipeline is scheduled, meaning it is owned by some thread.
         *
         * the next possible states are:
         * - unscheduled (everything got written; we are done)
         * - scheduled: new writes got detected
         * - reschedule: needed if one of the handlers wants to reschedule the pipeline
         */
        SCHEDULED,
        /*
         * One of the handler wants to stop with the pipeline; one of the usages is TLS handshake.
         * Additional writes of frames will not lead to a scheduling of the pipeline. Only a
         * wakeup will schedule the pipeline.
         *
         * Next possible states are:
         * - unscheduled: everything got written
         * - scheduled: new writes got detected
         * - reschedule: pipeline needs to be reprocessed
         * - blocked (one of the handler wants to stop with the pipeline; one of the usages is TLS handshake
         */
        BLOCKED,
        /*
         * state needed for pipeline that was scheduled, but needs to be reprocessed
         * this is needed for wakeup during processing (TLS).
         *
         * Next possible states are:
         * - unscheduled: everything got written
         * - scheduled: new writes got detected
         * - reschedule: pipeline needs to be reprocessed
         * - blocked (one of the handler wants to stop with the pipeline; one of the usages is TLS handshake
         */
        RESCHEDULE
    }

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> priorityWriteQueue = new ConcurrentLinkedQueue<>();

    private OutboundHandler[] handlers = new OutboundHandler[0];
    private ByteBuffer sendBuffer;

    private final AtomicReference<State> scheduled = new AtomicReference<>(State.SCHEDULED);
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
    private final ConcurrencyDetection concurrencyDetection;
    private final boolean writeThroughEnabled;

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

    @Probe
    private long scheduled() {
        return scheduled.get().ordinal();
    }

    public void write(OutboundFrame frame) {
        if (frame.isUrgent()) {
            priorityWriteQueue.offer(frame);
        } else {
            writeQueue.offer(frame);
        }

        // take care of the scheduling.
        for (; ; ) {
            State state = scheduled.get();
            if (state == State.UNSCHEDULED) {
                // pipeline isn't scheduled, so we need to schedule it.
                if (!scheduled.compareAndSet(State.UNSCHEDULED, State.SCHEDULED)) {
                    // try again
                    continue;
                }

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
                    ownerAddTaskAndWakeup(this);
                }
                return;
            } else if (state == State.SCHEDULED || state == State.RESCHEDULE) {
                // already scheduled, so we are done
                if (writeThroughEnabled) {
                    concurrencyDetection.onDetected();
                }
                return;
            } else if (state == State.BLOCKED) {
                // pipeline is blocked, so we don't need to schedule
                return;
            } else {
                throw new IllegalStateException("Unexpected state:" + state);
            }
        }
    }

    @Override
    public OutboundPipeline wakeup() {
        for (; ; ) {
            State prevState = scheduled.get();
            if (prevState == State.RESCHEDULE) {
                break;
            } else {
                  if (scheduled.compareAndSet(prevState, State.RESCHEDULE)) {
                    if (prevState == State.UNSCHEDULED || prevState == State.BLOCKED) {
                        ownerAddTaskAndWakeup(this);
                    }
                    break;
                }
            }
        }

        return this;
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

    // is never called concurrently!
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
                // change in the pipeline detected, therefor the loop is restarted.
                localHandlers = handlers;
                pipelineStatus = CLEAN;
                handlerIndex = -1;
            } else if (handlerStatus != CLEAN) {
                pipelineStatus = handlerStatus;
            }
        }

        flushToSocket();

        if (migrationRequested()) {
            startMigration();
            // we leave this method and the NioOutboundPipeline remains scheduled.
            // So we don't need to worry about write-through
            return;
        }

        if (sendBuffer.remaining() > 0) {
            pipelineStatus = DIRTY;
        }

        switch (pipelineStatus) {
            case CLEAN:
                postProcessClean();
                break;
            case DIRTY:
                postProcessDirty();
                break;
            case BLOCKED:
                postProcessBlocked();
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void postProcessBlocked() throws IOException {
        // pipeline is blocked; no point in receiving OP_WRITE events.
        unregisterOp(OP_WRITE);

        // we try to set the state to blocked.
        for (; ; ) {
            State state = scheduled.get();
            if (state == State.SCHEDULED) {
                // if it is still scheduled, we'll just try to put it to blocked.
                if (scheduled.compareAndSet(State.SCHEDULED, State.BLOCKED)) {
                    break;
                }
            } else if (state == State.BLOCKED) {
                // it is already blocked, so we are done.
                break;
            } else if (state == State.RESCHEDULE) {
                // rescheduling is requested, so lets do that. Once put to RESCHEDULE,
                // only the thread running the process method will change the state, so we can safely call a set.
                scheduled.set(State.SCHEDULED);
                // this will cause the pipeline to be rescheduled.
                owner().addTaskAndWakeup(this);
                break;
            } else {
                throw new IllegalStateException("unexpected state:" + state);
            }
        }
    }

    private void postProcessDirty() throws IOException {
        // pipeline is dirty, so register for an OP_WRITE to write more data.
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
    }

    private void postProcessClean() throws IOException {
        // There is nothing left to be done; so lets unschedule this pipeline
        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(OP_WRITE);

        for (; ; ) {
            State state = scheduled.get();
            if (state == State.RESCHEDULE) {
                // the pipeline needs to be rescheduled. The current thread is still owner of the pipeline,
                // so lets remove the reschedule flag and return it to schedule and lets reprocess the pipeline.
                scheduled.set(State.SCHEDULED);
                owner().addTaskAndWakeup(this);
                return;
            }

            if (!scheduled.compareAndSet(state, State.UNSCHEDULED)) {
                // we didn't manage to set it to unscheduled, lets retry the loop and see what needs to be done
                continue;
            }

            // we manage to unschedule the pipeline. From this point on we have released ownership of the pipeline
            // and another thread could call the process method.
            if (writeQueue.isEmpty() && priorityWriteQueue.isEmpty()) {
                //pipeline is clean, we are done.
                return;
            }

            // there is stuff to write we are going to reclaim ownership of the pipeline to prevent
            // we are going to end up with scheduled pipeline that isn't going to be processed.
            // If we can't reclaim ownership, then it is the concern of the other thread deal with the pipeline.
            if (scheduled.compareAndSet(State.UNSCHEDULED, State.SCHEDULED)) {
                if (Thread.currentThread().getClass() == NioThread.class) {
                    owner().addTask(this);
                } else {
                    owner().addTaskAndWakeup(this);
                }
            }

            return;
        }
    }

    private void flushToSocket() throws IOException {
        lastWriteTime = currentTimeMillis();
        int written = socketChannel.write(sendBuffer);
        bytesWritten.inc(written);
        //System.out.println(channel + " bytes written:" + written);
    }

    void drainWriteQueues() {
        writeQueue.clear();
        priorityWriteQueue.clear();
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
}

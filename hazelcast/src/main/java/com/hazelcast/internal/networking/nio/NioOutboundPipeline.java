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
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.WriteResult;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * The {@link NioOutboundPipeline} is the pipeline for outbound traffic. The {@link NioOutboundPipeline} itself is the first
 * item in the pipeline and it will take packets from one of its queues that need to be send and pushes that down the pipeline.
 *
 * Warning:
 * In theory the SocketWriter could have been the last item in the pipeline. The problem with using this approach are:
 * - overhead: if you would have e.g. a 64kb buffer and have 64 messages of 1kb pending, then there would be 64 socket.write calls and
 * this is going to give a huge amount of overhead.
 * - starvation: if a single handler would have a continous stream of frames to write, and the socket is able to keep up,
 * then this handler would not release the IO thread.
 */
public final class NioOutboundPipeline
        extends ChannelOutboundHandler
        implements NioPipeline, Closeable, Runnable {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<OutboundFrame>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> urgentWriteQueue = new ConcurrentLinkedQueue<OutboundFrame>();

    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();

    private final NioThread nioThread;
    // shows the ID of the ioThread that is currently owning the handler
    @Probe
    private volatile int nioThreadId;
    private final SocketChannel socketChannel;
    private final ILogger logger;
    private final IOBalancer balancer;
    @Probe
    protected final SwCounter handleCount = newSwCounter();

    // todo: the meaning of this value is questionable. It only says when this handler was called the last time, but
    // not when something was written to the socket.
    private volatile long lastWriteTimeMillis;

    // this field will be accessed by the NioThread or
    // it is accessed by any other thread but only that thread managed to cas the scheduled flag to true.
    // This prevents running into an NioThread that is migrating.
    private NioThread newOwner;

    private long bytesReadLastPublish;
    private long normalFramesReadLastPublish;
    private long priorityFramesReadLastPublish;
    private long eventsLastPublish;
    private SelectionKey selectionKey;

    public NioOutboundPipeline(NioChannel channel,
                               NioThread nioThread,
                               ILogger logger,
                               IOBalancer balancer) {
        this.nioThread = nioThread;
        this.nioThreadId = nioThread.id;
        this.channel = channel;
        this.socketChannel = channel.socketChannel();
        this.logger = logger;
        this.balancer = balancer;
    }

    public int totalFramesPending() {
        return writeQueue.size() + urgentWriteQueue.size();
    }

    public long lastWriteTimeMillis() {
        return lastWriteTimeMillis;
    }

    @Probe(name = "writeQueuePendingBytes", level = DEBUG)
    public long bytesPending() {
        return bytesPending(writeQueue);
    }

    @Probe(name = "priorityWriteQueuePendingBytes", level = DEBUG)
    public long priorityBytesPending() {
        return bytesPending(urgentWriteQueue);
    }

    private long bytesPending(Queue<OutboundFrame> writeQueue) {
        long bytesPending = 0;
        for (OutboundFrame frame : writeQueue) {
            if (frame instanceof Packet) {
                bytesPending += ((Packet) frame).packetSize();
            }
        }
        return bytesPending;
    }

    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastWriteTimeMillis, 0);
    }

    @Probe(level = DEBUG)
    private long isScheduled() {
        return scheduled.get() ? 1 : 0;
    }

    @Probe(level = DEBUG)
    private long opsInterested() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(level = DEBUG)
    private long opsReady() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    @Override
    public NioThread getOwner() {
        return nioThread;
    }

    @Override
    public void onFailure(Throwable e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        //todo: should this stay here?
        //todo: should this be done silently?
        if (selectionKey != null) {
            selectionKey.cancel();
        }

        nioThread.getErrorHandler().onError(channel, e);
    }

    @Override
    public void renewSelectionKey() throws ClosedChannelException {
        if (selectionKey != null) {
            selectionKey.cancel();
        }
        selectionKey = socketChannel.register(nioThread.getSelector(), OP_WRITE, this);
    }

    private void registerOpWrite() {
        // todo: make symmetric with unregisterOpWrite
        selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);
    }

    private void unregisterOpWrite() {
        int interestOps = selectionKey.interestOps();
        if ((interestOps & OP_WRITE) != 0) {
            selectionKey.interestOps(interestOps & ~OP_WRITE);
        }
    }

    @Override
    public long getLoad() {
        switch (LOAD_TYPE) {
            case LOAD_BALANCING_HANDLE:
                return handleCount.get();
            case LOAD_BALANCING_BYTE:
                return bytesWritten.get() + priorityFramesWritten.get();
            case LOAD_BALANCING_FRAME:
                return normalFramesWritten.get() + priorityFramesWritten.get();
            default:
                throw new RuntimeException();
        }
    }

    public void flush() {
        nioThread.addTaskAndWakeup(this);
    }

    public void write(OutboundFrame frame) {
        //System.out.println(channel + " offer " + frame);

        if (frame.isUrgent()) {
            urgentWriteQueue.offer(frame);
        } else {
            writeQueue.offer(frame);
        }

        schedule();
    }

    /**
     * Makes sure this pipeline is scheduled to be executed by the IO thread.
     * <p/>
     * This call is made by 'outside' threads that interact with the connection. For example when a frame is placed
     * on the connection to be written. It will never be made by an IO thread.
     * <p/>
     * If the pipeline already is scheduled, the call is ignored.
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

        // We managed to schedule the pipeline. This means we need to add a task to
        // the nioThread and give it a kick so that it processes our frames.
        nioThread.addTaskAndWakeup(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public WriteResult onWrite() throws Exception {
        System.out.println(channel + " NioOutboundPipeline.onWrite start");

        //Thread.sleep(500);

        handleCount.inc();
        lastWriteTimeMillis = currentTimeMillis();

        boolean dirtyPipeline = pushFramesThroughPipeline();
        boolean dirtySocketBuffer = writeSocketBufferToSocket();

        // todo: what would happen if a pipeline is dirty. So it could be that all the content that ends up in the last
        // bytebuffer is written; but some data remains in the pipeline

        //

        if (newOwner != null) {
            //System.out.println(channel + " NioOutboundPipeline.onWrite migration");

            // Migration is needed. If socketBuffer or pipeline is dirty, the new owner will take care of it because
            // it will automatically reschedule to pipeline.

            startMigration();
        } else if (dirtyPipeline || dirtySocketBuffer) {
            //System.out.println(channel + " NioOutboundPipeline.onWrite OP_WRITE");

            // so the pipeline is dirty or the socketBuffer is dirty; this means we need to register for an OP_WRITE
            // to get the remaining content flushed.

            registerOpWrite();
        } else {
            //System.out.println(channel + " NioOutboundPipeline.onWrite unschedule");

            // If a handler is blocked, and the trailing handlers are not dirty and the socketbuffer is not dirty,
            // we need to unschedule.
            // What is the difference between clean and blocked? So the dirty makes sense since we can see if the
            // pipeline needs to be rescheduled. But why make distinction between CLEAN/BLOCKED?

            // It is related to the chain of handlers.
            // So a handler returns blocked or returns clean
            // how does the subsequent handler deal with it?

            unschedule();
        }

        return null;
    }

    private boolean pushFramesThroughPipeline() throws Exception {
        // dirty means that not everything got written
        boolean dirty;
        for (; ; ) {
            boolean polled;
            if (next.frame == null) {
                OutboundFrame frame = poll();
                if (frame == null) {
                    polled = false;
                } else {
                    next.frame = frame;
                    polled = true;
                }
            } else {
                // so there is still a frame pending to be written and therefor polling isn't needed.
                polled = false;
            }

            dirty = writePipeline();

            if (!polled) {
                break;
            }

            //System.out.println(channel + " NioOutboundPipeline.onWrite loop");
        }
        return dirty;
    }


    private OutboundFrame poll() {
        for (; ; ) {
            boolean urgent = true;
            OutboundFrame frame = urgentWriteQueue.poll();

            if (frame == null) {
                urgent = false;
                frame = writeQueue.poll();
            }

            if (frame == null) {
                return null;
            }

            if (frame.getClass() == TaskFrame.class) {
                TaskFrame taskFrame = (TaskFrame) frame;
                taskFrame.task.run();
                continue;
            }

            if (urgent) {
                priorityFramesWritten.inc();
            } else {
                normalFramesWritten.inc();
            }

            return frame;
        }
    }

    private ByteBuffer socketBuffer;

    /**
     * return true als the pipeline dirty is.
     */
    private boolean writePipeline() throws Exception {
        System.out.println(channel + " write pipeline start " + pipelineToString());

        ChannelOutboundHandler handler = next;
        Boolean dirty = null;
        do {
            socketBuffer = handler.dst;
            //System.out.println(handler);
            WriteResult result = handler.onWrite();
            System.out.println(channel + " write pipeline " + handler.getClass().getSimpleName() + " " + result);
            switch (result) {
                case CLEAN:
                    if (dirty == null) {
                        dirty = false;
                    }
                    break;
                case DIRTY:
                    dirty = true;
                    break;
                case BLOCKED:
                    dirty = false;
                    break;
                default:
                    throw new RuntimeException();
            }

            handler = handler.next;
        } while (handler != null);

        return dirty;
    }

    /**
     * returns true if the socketBuffer is dirty; so not all data got written.
     */
    private boolean writeSocketBufferToSocket() throws IOException {
        ByteBuffer dst = socketBuffer;

        if (dst == null || dst.position() <= 0) {
            return false;
        }

        // So there is data for writing, so lets prepare the buffer for writing and then write it to the channel.
        dst.flip();

        int written = socketChannel.write(dst);

        bytesWritten.inc(written);

        System.out.println(channel + " SocketWriter.write bytes written " + written);

        compactOrClear(dst);

        return dst.position() > 0;
    }

    /**
     * Tries to unschedule this pipeline. This call should be made when the pipeline can't progress because
     * there is nothing to write.
     *
     * This call is only made by the owning IO thread.
     */
    private void unschedule() {
        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOpWrite();

        scheduled.set(false);

        // todo: so we check the queue's after we unschedule; why not do it before as well? Could prevent an unwanted unscheduling/rescheduling
        if (writeQueue.isEmpty() && urgentWriteQueue.isEmpty()) {
            // there are no remaining frames, so we are done.
            return;
        }

        // So there are frames, but we just unscheduled ourselves. If we don't try to reschedule, then these
        // Frames are at risk not to be send.
        if (!scheduled.compareAndSet(false, true)) {
            //someone else managed to schedule this ChannelOutboundHandler, so we are done.
            return;
        }

        // We managed to reschedule. So lets add ourselves to the nioThread so we are processed again.
        // We don't need to call wakeup because the current thread is the IO-thread and the selectionQueue will be processed
        // till it is empty. So it will also pick up tasks that are added while it is processing the selectionQueue.
        nioThread.addTask(NioOutboundPipeline.this);
    }

    private void startMigration() throws IOException {
        NioThread newOwner = this.newOwner;
        this.newOwner = null;
        //startMigration(newOwner);
    }

    @Override
    public void run() {
        try {
            onWrite();
        } catch (Throwable t) {
            onFailure(t);
        }
    }

    @Override
    public void close() {
        writeQueue.clear();
        urgentWriteQueue.clear();
    }

    @Override
    public void requestMigration(NioThread newOwner) {
        write(new TaskFrame(new StartMigrationTask(newOwner)));
    }

    @Override
    public String toString() {
        return channel + ".channelWriter";
    }

    public String pipelineToString() {
        StringBuilder sb = new StringBuilder("[");
        ChannelOutboundHandler h = this;
        boolean first = true;
        while (h != null) {
            if (first) {
                first = false;
            } else {
                sb.append("->");
            }
            sb.append(h.getClass().getSimpleName());
            h = h.next;
        }
        sb.append("]");
        return sb.toString();
    }

    protected void publish() {
        nioThread.bytesTransceived += bytesWritten.get() - bytesReadLastPublish;
        nioThread.framesTransceived += normalFramesWritten.get() - normalFramesReadLastPublish;
        nioThread.priorityFramesTransceived += priorityFramesWritten.get() - priorityFramesReadLastPublish;
        nioThread.handleCount += handleCount.get() - eventsLastPublish;

        bytesReadLastPublish = bytesWritten.get();
        normalFramesReadLastPublish = normalFramesWritten.get();
        priorityFramesReadLastPublish = priorityFramesWritten.get();
        eventsLastPublish = handleCount.get();
    }

    public void start() {
        nioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    renewSelectionKey();
                } catch (Throwable t) {
                    onFailure(t);
                }
            }
        });
    }

    /**
     * The TaskFrame is not really a Frame. It is a way to put a task on one of the frame-queues. Using this approach we
     * can lift on top of the Frame scheduling mechanism and we can prevent having:
     * - multiple NioThread-tasks for a ChannelWriter on multiple NioThread
     * - multiple NioThread-tasks for a ChannelWriter on the same NioThread.
     */
    private static final class TaskFrame implements OutboundFrame {

        private final Runnable task;

        private TaskFrame(Runnable task) {
            this.task = task;
        }

        @Override
        public boolean isUrgent() {
            return true;
        }
    }

    /**
     * Triggers the migration when executed by setting the ChannelWriter.newOwner field. When the handle method completes, it
     * checks if this field if set, if so, the migration starts.
     *
     * If the current nioThread is the same as 'theNewOwner' then the call is ignored.
     */
    private final class StartMigrationTask implements Runnable {
        // field is called 'theNewOwner' to prevent any ambiguity problems with the outboundHandler.newOwner.
        // Else you get a lot of ugly ChannelOutboundHandler.this.newOwner is ...
        private final NioThread theNewOwner;

        StartMigrationTask(NioThread theNewOwner) {
            this.theNewOwner = theNewOwner;
        }

        @Override
        public void run() {
            assert newOwner == null : "No migration can be in progress";

            if (nioThread == theNewOwner) {
                // if there is no change, we are done
                return;
            }

            newOwner = theNewOwner;
        }
    }
}

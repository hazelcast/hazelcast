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

package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.internal.networking.SocketWriterInitializer;
import com.hazelcast.internal.networking.WriteHandler;
import com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.EmptyStatement.ignore;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * The writing side of the {@link TcpIpConnection}.
 */
public final class NonBlockingSocketWriter
        extends AbstractHandler
        implements Runnable, SocketWriter {

    private static final long TIMEOUT = 3;


    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<OutboundFrame>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> urgentWriteQueue = new ConcurrentLinkedQueue<OutboundFrame>();
    private final SocketWriterInitializer initializer;

    private ByteBuffer outputBuffer;

    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();
    private WriteHandler writeHandler;

    private OutboundFrame currentFrame;
    private volatile long lastWriteTime;

    // this field will be accessed by the NonBlockingIOThread or
    // it is accessed by any other thread but only that thread managed to cas the scheduled flag to true.
    // This prevents running into an NonBlockingIOThread that is migrating.
    private NonBlockingIOThread newOwner;

    private long bytesReadLastPublish;
    private long normalFramesReadLastPublish;
    private long priorityFramesReadLastPublish;
    private long eventsLastPublish;

    public NonBlockingSocketWriter(SocketConnection connection,
                                   NonBlockingIOThread ioThread,
                                   ILogger logger,
                                   IOBalancer balancer,
                                   SocketWriterInitializer initializer) {
        super(connection, ioThread, OP_WRITE, logger, balancer);
        this.initializer = initializer;
    }

    @Override
    public long getEventCount() {
        switch (LOAD_TYPE) {
            case 0:
                return handleCount.get();
            case 1:
                return bytesWritten.get() + priorityFramesWritten.get();
            case 2:
                return normalFramesWritten.get() + priorityFramesWritten.get();
            default:
                throw new RuntimeException();
        }
    }

    @Override
    public int totalFramesPending() {
        return writeQueue.size() + urgentWriteQueue.size();
    }

    @Override
    public long lastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Override
    public WriteHandler getWriteHandler() {
        return writeHandler;
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
        return max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Probe(level = DEBUG)
    private long isScheduled() {
        return scheduled.get() ? 1 : 0;
    }

    // accessed from ReadHandler and SocketConnector
    @Override
    public void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    if (writeHandler == null) {
                        initializer.init(connection, NonBlockingSocketWriter.this, protocol);
                    }
                } catch (Throwable t) {
                    onFailure(t);
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            latch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    @Override
    public void initOutputBuffer(ByteBuffer outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    @Override
    public void write(OutboundFrame frame) {
        if (frame.isUrgent()) {
            urgentWriteQueue.offer(frame);
        } else {
            writeQueue.offer(frame);
        }

        schedule();
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

    /**
     * Makes sure this WriteHandler is scheduled to be executed by the IO thread.
     * <p/>
     * This call is made by 'outside' threads that interact with the connection. For example when a frame is placed
     * on the connection to be written. It will never be made by an IO thread.
     * <p/>
     * If the WriteHandler already is scheduled, the call is ignored.
     */
    private void schedule() {
        if (scheduled.get()) {
            // So this WriteHandler is still scheduled, we don't need to schedule it again
            return;
        }

        if (!scheduled.compareAndSet(false, true)) {
            // Another thread already has scheduled this WriteHandler, we are done. It
            // doesn't matter which thread does the scheduling, as long as it happens.
            return;
        }

        // We managed to schedule this WriteHandler. This means we need to add a task to
        // the ioThread and give it a kick so that it processes our frames.
        ioThread.addTaskAndWakeup(this);
    }

    /**
     * Tries to unschedule this WriteHandler.
     * <p/>
     * It will only be unscheduled if:
     * - the outputBuffer is empty
     * - there are no pending frames.
     * <p/>
     * If the outputBuffer is dirty then it will register itself for an OP_WRITE since we are interested in knowing
     * if there is more space in the socket output buffer.
     * If the outputBuffer is not dirty, then it will unregister itself from an OP_WRITE since it isn't interested
     * in space in the socket outputBuffer.
     * <p/>
     * This call is only made by the IO thread.
     */
    private void unschedule() throws IOException {
        if (dirtyOutputBuffer() || currentFrame != null) {
            // Because not all data was written to the socket, we need to register for OP_WRITE so we get
            // notified when the socketChannel is ready for more data.
            registerOp(OP_WRITE);

            // If the outputBuffer is not empty, we don't need to unschedule ourselves. This is because the
            // WriteHandler will be triggered by a nio write event to continue sending data.
            return;
        }

        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(OP_WRITE);
        // So the outputBuffer is empty, so we are going to unschedule ourselves.
        scheduled.set(false);

        if (writeQueue.isEmpty() && urgentWriteQueue.isEmpty()) {
            // there are no remaining frames, so we are done.
            return;
        }

        // So there are frames, but we just unscheduled ourselves. If we don't try to reschedule, then these
        // Frames are at risk not to be send.
        if (!scheduled.compareAndSet(false, true)) {
            //someone else managed to schedule this WriteHandler, so we are done.
            return;
        }

        // We managed to reschedule. So lets add ourselves to the ioThread so we are processed again.
        // We don't need to call wakeup because the current thread is the IO-thread and the selectionQueue will be processed
        // till it is empty. So it will also pick up tasks that are added while it is processing the selectionQueue.
        ioThread.addTask(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handle() throws Exception {
        handleCount.inc();
        lastWriteTime = currentTimeMillis();

        if (writeHandler == null) {
            initializer.init(connection, this, CLUSTER);
            registerOp(OP_WRITE);
        }

        fillOutputBuffer();

        if (dirtyOutputBuffer()) {
            writeOutputBufferToSocket();
        }

        if (newOwner == null) {
            unschedule();
        } else {
            startMigration();
        }
    }

    @Override
    public void initWriteHandler(WriteHandler writeHandler) {
        this.writeHandler = writeHandler;
    }

    private void startMigration() throws IOException {
        NonBlockingIOThread newOwner = this.newOwner;
        this.newOwner = null;
        startMigration(newOwner);
    }

    /**
     * Checks of the outputBuffer is dirty.
     *
     * @return true if dirty, false otherwise.
     */
    private boolean dirtyOutputBuffer() {
        return outputBuffer.position() > 0;
    }

    /**
     * Writes to content of the outputBuffer to the socket.
     */
    private void writeOutputBufferToSocket() throws IOException {
        // So there is data for writing, so lets prepare the buffer for writing and then write it to the socketChannel.
        outputBuffer.flip();
        int written = socketChannel.write(outputBuffer);

        bytesWritten.inc(written);

        // Now we verify if all data is written.
        if (outputBuffer.hasRemaining()) {
            // We did not manage to write all data to the socket. So lets compact the buffer so new data can be added at the end.
            outputBuffer.compact();
        } else {
            // We managed to fully write the outputBuffer to the socket, so we are done.
            outputBuffer.clear();
        }
    }

    /**
     * Fills the outBuffer with frames. This is done till there are no more frames or till there is no more space in the
     * outputBuffer.
     */
    private void fillOutputBuffer() throws Exception {
        if (currentFrame == null) {
            // there is no pending frame, lets poll one.
            currentFrame = poll();
        }

        while (currentFrame != null) {
            // Lets write the currentFrame to the outputBuffer.
            if (!writeHandler.onWrite(currentFrame, outputBuffer)) {
                // We are done for this round because not all data of the currentFrame fits in the outputBuffer
                return;
            }

            // The current frame has been written completely. So lets poll for another one.
            currentFrame = poll();
        }
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (Throwable t) {
            onFailure(t);
        }
    }

    @Override
    public void close() {
        writeQueue.clear();
        urgentWriteQueue.clear();

        CloseTask closeTask = new CloseTask();
        write(new TaskFrame(closeTask));
        closeTask.awaitCompletion();
    }

    @Override
    public void requestMigration(NonBlockingIOThread newOwner) {
        write(new TaskFrame(new StartMigrationTask(newOwner)));
    }

    @Override
    public String toString() {
        return connection + ".socketWriter";
    }

    /**
     * The TaskFrame is not really a Frame. It is a way to put a task on one of the frame-queues. Using this approach we
     * can lift on top of the Frame scheduling mechanism and we can prevent having:
     * - multiple NonBlockingIOThread-tasks for a SocketWriter on multiple NonBlockingIOThread
     * - multiple NonBlockingIOThread-tasks for a SocketWriter on the same NonBlockingIOThread.
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
     * Triggers the migration when executed by setting the SocketWriter.newOwner field. When the handle method completes, it
     * checks if this field if set, if so, the migration starts.
     *
     * If the current ioThread is the same as 'theNewOwner' then the call is ignored.
     */
    private final class StartMigrationTask implements Runnable {
        // field is called 'theNewOwner' to prevent any ambiguity problems with the writeHandler.newOwner.
        // Else you get a lot of ugly WriteHandler.this.newOwner is ...
        private final NonBlockingIOThread theNewOwner;

        StartMigrationTask(NonBlockingIOThread theNewOwner) {
            this.theNewOwner = theNewOwner;
        }

        @Override
        public void run() {
            assert newOwner == null : "No migration can be in progress";

            if (ioThread == theNewOwner) {
                // if there is no change, we are done
                return;
            }

            newOwner = theNewOwner;
        }
    }

    @Override
    protected void publish() {
        ioThread.bytesTransceived += bytesWritten.get() - bytesReadLastPublish;
        ioThread.framesTransceived += normalFramesWritten.get() - normalFramesReadLastPublish;
        ioThread.priorityFramesTransceived += priorityFramesWritten.get() - priorityFramesReadLastPublish;
        ioThread.handleCount += handleCount.get() - eventsLastPublish;

        bytesReadLastPublish = bytesWritten.get();
        normalFramesReadLastPublish = normalFramesWritten.get();
        priorityFramesReadLastPublish = priorityFramesWritten.get();
        eventsLastPublish = handleCount.get();
    }

    private class CloseTask implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() {
            try {
                socketChannel.closeOutbound();
            } catch (IOException e) {
                logger.finest("Error while closing outbound", e);
            } finally {
                latch.countDown();
            }
        }

        void awaitCompletion() {
            try {
                latch.await(TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }
}

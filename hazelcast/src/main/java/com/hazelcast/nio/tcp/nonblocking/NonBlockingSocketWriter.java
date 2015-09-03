/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Frame;
import com.hazelcast.nio.ascii.TextWriteHandler;
import com.hazelcast.nio.tcp.NewClientWriteHandler;
import com.hazelcast.nio.tcp.OldClientWriteHandler;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.util.counters.SwCounter;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * The writing side of the {@link TcpIpConnection}.
 */
public final class NonBlockingSocketWriter extends AbstractHandler implements Runnable, SocketWriter {

    private static final long TIMEOUT = 3;

    @Probe(name = "out.eventCount")
    private final SwCounter eventCount = newSwCounter();
    @Probe(name = "out.writeQueueSize")
    private final Queue<Frame> writeQueue = new ConcurrentLinkedQueue<Frame>();
    @Probe(name = "out.priorityWriteQueueSize")
    private final Queue<Frame> urgentWriteQueue = new ConcurrentLinkedQueue<Frame>();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private ByteBuffer outputBuffer;
    @Probe(name = "out.bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "out.normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "out.priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();
    private final MetricsRegistry metricsRegistry;

    private volatile Frame currentFrame;
    private WriteHandler writeHandler;
    private volatile long lastWriteTime;

    private boolean shutdown;
    // this field will be accessed by the NonBlockingIOThread or
    // it is accessed by any other thread but only that thread managed to cas the scheduled flag to true.
    // This prevents running into an NonBlockingIOThread that is migrating.
    private NonBlockingIOThread newOwner;

    NonBlockingSocketWriter(TcpIpConnection connection, NonBlockingIOThread ioThread, MetricsRegistry metricsRegistry) {
        super(connection, ioThread, SelectionKey.OP_WRITE);

        // sensors
        this.metricsRegistry = metricsRegistry;
        metricsRegistry.scanAndRegister(this, "tcp.connection[" + connection.getMetricsId() + "]");
    }

    @Probe(name = "out.interestedOps")
    private long interestOps() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(name = "out.readyOps")
    private long readyOps() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    @Override
    public int totalFramesPending() {
        return writeQueue.size() + urgentWriteQueue.size();
    }

    @Override
    public long getLastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Override
    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    @Probe(name = "out.writeQueuePendingBytes")
    public long bytesPending() {
        return bytesPending(writeQueue);
    }

    @Probe(name = "out.priorityWriteQueuePendingBytes")
    public long priorityBytesPending() {
        return bytesPending(urgentWriteQueue);
    }

    private long bytesPending(Queue<Frame> writeQueue) {
        long bytesPending = 0;
        for (Frame frame : writeQueue) {
            if (frame instanceof Packet) {
                bytesPending += ((Packet) frame).packetSize();
            }
        }
        return bytesPending;
    }

    @Probe(name = "out.idleTimeMs")
    private long idleTimeMs() {
        return Math.max(System.currentTimeMillis() - lastWriteTime, 0);
    }

    @Probe(name = "out.isScheduled")
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
                createWriterHandler(protocol);
                latch.countDown();
            }
        });
        try {
            latch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    private void createWriterHandler(String protocol) {
        if (writeHandler == null) {
            if (CLUSTER.equals(protocol)) {
                configureBuffers(ioService.getSocketSendBufferSize() * KILO_BYTE);
                writeHandler = ioService.createWriteHandler(connection);
                outputBuffer.put(stringToBytes(CLUSTER));
                registerOp(SelectionKey.OP_WRITE);
            } else if (CLIENT_BINARY.equals(protocol)) {
                configureBuffers(ioService.getSocketClientSendBufferSize() * KILO_BYTE);
                writeHandler = new OldClientWriteHandler();
            } else if (CLIENT_BINARY_NEW.equals(protocol)) {
                configureBuffers(ioService.getSocketClientReceiveBufferSize() * KILO_BYTE);
                writeHandler = new NewClientWriteHandler();
            } else {
                configureBuffers(ioService.getSocketClientSendBufferSize() * KILO_BYTE);
                writeHandler = new TextWriteHandler(connection);
            }
        }
    }

    private void configureBuffers(int size) {
        outputBuffer = ByteBuffer.allocate(size);
        try {
            connection.setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + connection + " to "
                    + size + " B.", e);
        }
    }

    @Override
    public void offer(Frame frame) {
        if (frame.isUrgent()) {
            urgentWriteQueue.offer(frame);
        } else {
            writeQueue.offer(frame);
        }

        schedule();
    }

    private Frame poll() {
        for (; ; ) {
            boolean urgent = true;
            Frame frame = urgentWriteQueue.poll();

            if (frame == null) {
                urgent = false;
                frame = writeQueue.poll();
            }

            if (frame == null) {
                return null;
            }

            if (frame instanceof TaskFrame) {
                ((TaskFrame) frame).run();
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
    private void unschedule() {
        if (dirtyOutputBuffer() || currentFrame != null) {
            // Because not all data was written to the socket, we need to register for OP_WRITE so we get
            // notified when the socketChannel is ready for more data.
            registerOp(SelectionKey.OP_WRITE);

            // If the outputBuffer is not empty, we don't need to unschedule ourselves. This is because the
            // WriteHandler will be triggered by a nio write event to continue sending data.
            return;
        }

        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(SelectionKey.OP_WRITE);
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
    public long getEventCount() {
        return eventCount.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handle() throws Exception {
        eventCount.inc();
        lastWriteTime = currentTimeMillis();

        if (shutdown) {
            return;
        }

        if (writeHandler == null) {
            logger.log(Level.WARNING, "SocketWriter is not set, creating SocketWriter with CLUSTER protocol!");
            createWriterHandler(CLUSTER);
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

    private void startMigration() {
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
     *
     * @throws Exception
     */
    private void writeOutputBufferToSocket() throws Exception {
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
     *
     * @throws Exception
     */
    private void fillOutputBuffer() throws Exception {
        for (; ; ) {
            if (!outputBuffer.hasRemaining()) {
                // The buffer is completely filled, we are done.
                return;
            }

            // If there currently is not frame sending, lets try to get one.
            if (currentFrame == null) {
                currentFrame = poll();
                if (currentFrame == null) {
                    // There is no frames to write, we are done.
                    return;
                }
            }

            // Lets write the currentFrame to the outputBuffer.
            if (!writeHandler.onWrite(currentFrame, outputBuffer)) {
                // We are done for this round because not all data of the current frame fits in the outputBuffer
                return;
            }

            // The current frame has been written completely. So lets null it and lets try to write another frame.
            currentFrame = null;
        }
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (Throwable e) {
            ioThread.handleSelectionKeyFailure(e);
        }
    }

    @Override
    public void shutdown() {
        metricsRegistry.deregister(this);
        writeQueue.clear();
        urgentWriteQueue.clear();

        ShutdownTask shutdownTask = new ShutdownTask();
        offer(shutdownTask);
        shutdownTask.awaitCompletion();
    }

    @Override
    public void start() {
        //no-op
    }

    @Override
    public void requestMigration(NonBlockingIOThread newOwner) {
        offer(new StartMigrationTask(newOwner));
    }

    @Override
    public String toString() {
        return connection + ".writeHandler";
    }

    /**
     * The TaskFrame is not really a Frame. It is a way to put a task on one of the frame-queues. Using this approach we
     * can lift on top of the Frame scheduling mechanism and we can prevent having:
     * - multiple NonBlockingIOThread-tasks for a SocketWriter on multiple NonBlockingIOThread
     * - multiple NonBlockingIOThread-tasks for a SocketWriter on the same NonBlockingIOThread.
     */
    private abstract class TaskFrame implements Frame {
        abstract void run();

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
    private class StartMigrationTask extends TaskFrame {
        // field is called 'theNewOwner' to prevent any ambiguity problems with the writeHandler.newOwner.
        // Else you get a lot of ugly WriteHandler.this.newOwner is ...
        private final NonBlockingIOThread theNewOwner;

        public StartMigrationTask(NonBlockingIOThread theNewOwner) {
            this.theNewOwner = theNewOwner;
        }

        @Override
        void run() {
            assert newOwner == null : "No migration can be in progress";

            if (ioThread == theNewOwner) {
                // if there is no change, we are done
                return;
            }

            newOwner = theNewOwner;
        }
    }

    private class ShutdownTask extends TaskFrame {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        void run() {
            shutdown = true;
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

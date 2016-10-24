/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.tcp.IOOutOfMemoryHandler;
import com.hazelcast.nio.tcp.SocketConnection;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.SocketWriterInitializer;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SpinningSocketWriter extends AbstractHandler implements SocketWriter {

    private static final long TIMEOUT = 3;

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue writeQueue = new ConcurrentLinkedQueue();

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue urgentWriteQueue = new ConcurrentLinkedQueue();

    private final SocketWriterInitializer initializer;
    private ByteBuffer outputBuffer;
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();
    private volatile long lastWriteTime;
    private WriteHandler writeHandler;
    private volatile byte[] currentFrame;
    private int position;

    public SpinningSocketWriter(SocketConnection connection,
                                ILogger logger,
                                IOOutOfMemoryHandler oomeHandler,
                                SocketWriterInitializer initializer) {
        super(connection, logger, oomeHandler);
        this.initializer = initializer;
    }

    @Override
    public void write(byte[] bytes, boolean urgent) {
        if (urgent) {
            urgentWriteQueue.add(bytes);
        } else {
            writeQueue.add(bytes);
        }
    }

    @Probe(name = "writeQueuePendingBytes")
    public long bytesPending() {
        return bytesPending(writeQueue);
    }

    @Probe(name = "priorityWriteQueuePendingBytes")
    public long priorityBytesPending() {
        return bytesPending(urgentWriteQueue);
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Override
    public int totalFramesPending() {
        return urgentWriteQueue.size() + writeQueue.size();
    }

    private long bytesPending(Queue<Object> writeQueue) {
        long bytesPending = 0;
        for (Object frame : writeQueue) {
            if (frame instanceof byte[]) {
                bytesPending += ((byte[]) frame).length;
            }
        }
        return bytesPending;
    }

    @Override
    public long lastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Override
    public void initWriteHandler(WriteHandler writeHandler) {
        this.writeHandler = writeHandler;
    }

    @Override
    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    // accessed from ReadHandler and SocketConnector
    @Override
    public void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        urgentWriteQueue.add(new TaskFrame(new Runnable() {
            @Override
            public void run() {
                logger.info("Setting protocol: " + protocol);
                if (writeHandler == null) {
                    initializer.init(connection, SpinningSocketWriter.this, protocol);
                }
                latch.countDown();
            }
        }));

        try {
            latch.await(TIMEOUT, SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    @Override
    public void initOutputBuffer(ByteBuffer outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    private byte[] poll() {
        for (; ; ) {
            boolean urgent = true;
            Object frame = urgentWriteQueue.poll();

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

            return (byte[]) frame;
        }
    }

    @Override
    public void close() {
        writeQueue.clear();
        urgentWriteQueue.clear();

        ShutdownTask shutdownTask = new ShutdownTask();
        // write(new TaskFrame(shutdownTask));
        shutdownTask.awaitCompletion();
    }

    public void write() throws Exception {
        if (!connection.isAlive()) {
            return;
        }

        if (writeHandler == null) {
            logger.log(Level.WARNING, "SocketWriter is not set, creating SocketWriter with CLUSTER protocol!");
            initializer.init(connection, this, CLUSTER);
            return;
        }

        fillOutputBuffer();

        if (dirtyOutputBuffer()) {
            writeOutputBufferToSocket();
        }
    }

    /**
     * Checks of the outputBuffer is dirty.
     *
     * @return true if dirty, false otherwise.
     */
    private boolean dirtyOutputBuffer() {
        if (outputBuffer == null) {
            return false;
        }
        return outputBuffer.position() > 0;
    }

    /**
     * Fills the outBuffer with frames. This is done till there are no more frames or till there is no more space in the
     * outputBuffer.
     *
     * @throws Exception
     */
    private void fillOutputBuffer() throws Exception {
        for (; ; ) {
            if (outputBuffer != null && !outputBuffer.hasRemaining()) {
                // The buffer is completely filled, we are done.
                return;
            }

            // If there currently is not frame sending, lets try to get one.
            if (currentFrame == null) {
                currentFrame = poll();
                if (currentFrame == null) {
                    // There is no frame to write, we are done.
                    return;
                }
            }

            // the number of bytes that can be written to the bb.
            int bytesWritable = outputBuffer.remaining();

            // the number of bytes that need to be written.
            int bytesNeeded = currentFrame.length - position;

            int bytesWrite;
            boolean done;
            if (bytesWritable >= bytesNeeded) {
                // All bytes for the value are available.
                bytesWrite = bytesNeeded;
                done = true;
            } else {
                // Not all bytes for the value are available. So lets write as much as is available.
                bytesWrite = bytesWritable;
                done = false;
            }

            outputBuffer.put(currentFrame, position, bytesWrite);
            position += bytesWrite;

            if (!done) {
                return;
            }


            // The current frame has been written completely. So lets null it and lets try to write another frame.
            currentFrame = null;
            position = 0;
        }
    }

    /**
     * Writes to content of the outputBuffer to the socket.
     *
     * @throws Exception
     */
    private void writeOutputBufferToSocket() throws Exception {
        // So there is data for writing, so lets prepare the buffer for writing and then write it to the socketChannel.
        outputBuffer.flip();
        int result = socketChannel.write(outputBuffer);
        if (result > 0) {
            lastWriteTime = currentTimeMillis();
            bytesWritten.inc(result);
        }

        if (outputBuffer.hasRemaining()) {
            outputBuffer.compact();
        } else {
            outputBuffer.clear();
        }
    }

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

    private class ShutdownTask implements Runnable {
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
                latch.await(TIMEOUT, SECONDS);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }
}

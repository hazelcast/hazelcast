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
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SpinningChannelWriter extends AbstractHandler {

    private static final long TIMEOUT = 3;

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<OutboundFrame>();

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> urgentWriteQueue = new ConcurrentLinkedQueue<OutboundFrame>();

    private final ChannelInitializer initializer;
    private ByteBuffer outputBuffer;
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();
    private volatile long lastWriteTime;
    private ChannelOutboundHandler outboundHandler;
    private volatile OutboundFrame currentFrame;

    public SpinningChannelWriter(Channel channel,
                                 ILogger logger,
                                 ChannelErrorHandler errorHandler,
                                 ChannelInitializer initializer) {
        super(channel, logger, errorHandler);
        this.initializer = initializer;
    }

    public void write(OutboundFrame frame) {
        if (frame.isUrgent()) {
            urgentWriteQueue.add(frame);
        } else {
            writeQueue.add(frame);
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

    @Probe
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastWriteTime, 0);
    }

    public int totalFramesPending() {
        return urgentWriteQueue.size() + writeQueue.size();
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

    public long lastWriteTimeMillis() {
        return lastWriteTime;
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

    public void close() {
        writeQueue.clear();
        urgentWriteQueue.clear();

        ShutdownTask shutdownTask = new ShutdownTask();
        write(new TaskFrame(shutdownTask));
        shutdownTask.awaitCompletion();
    }

    public void write() throws Exception {
        if (channel.isClosed()) {
            return;
        }

        if (outboundHandler == null && !init()) {
            return;
        }

        fillOutputBuffer();

        if (dirtyOutputBuffer()) {
            writeOutputBufferToSocket();
        }
    }

    /**
     * Tries to initialize.
     *
     * @return true if initialization was a success, false if insufficient data is available.
     * @throws IOException
     */
    private boolean init() throws IOException {
        InitResult<ChannelOutboundHandler> init = initializer.initOutbound(channel);
        if (init == null) {
            // we can't initialize the outbound-handler yet since insufficient data is available.
            return false;
        }

        this.outputBuffer = init.getByteBuffer();
        this.outboundHandler = init.getHandler();
        return true;
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

            // Lets write the currentFrame to the outputBuffer.
            if (!outboundHandler.onWrite(currentFrame, outputBuffer)) {
                // We are done for this round because not all data of the current frame fits in the outputBuffer
                return;
            }

            // The current frame has been written completely. So lets null it and lets try to write another frame.
            currentFrame = null;
        }
    }

    /**
     * Writes to content of the outputBuffer to the socket.
     *
     * @throws Exception
     */
    private void writeOutputBufferToSocket() throws Exception {
        // So there is data for writing, so lets prepare the buffer for writing and then write it to the channel.
        outputBuffer.flip();
        int result = channel.write(outputBuffer);
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
                channel.closeOutbound();
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

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

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.ascii.TextWriteHandler;
import com.hazelcast.nio.tcp.NewClientWriteHandler;
import com.hazelcast.nio.tcp.OldClientWriteHandler;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.counters.SwCounter;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static java.lang.System.currentTimeMillis;

public class SpinningSocketWriter extends AbstractHandler implements SocketWriter {

    private static final long TIMEOUT = 3;

    @Probe(name = "out.writeQueueSize")
    private final Queue<OutboundFrame> writeQueue;
    @Probe(name = "out.priorityWriteQueueSize")
    private final Queue<OutboundFrame> urgentWriteQueue;
    private final ILogger logger;
    private final SocketChannelWrapper socketChannel;
    private ByteBuffer outputBuffer;
    @Probe(name = "out.bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "out.normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "out.priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();
    private final MetricsRegistry metricsRegistry;
    private volatile long lastWriteTime;
    private WriteHandler writeHandler;
    private volatile OutboundFrame currentFrame;

    public SpinningSocketWriter(TcpIpConnection connection, MetricsRegistry metricsRegistry, ILogger logger) {
        super(connection, logger);
        this.metricsRegistry = metricsRegistry;
        this.logger = logger;
        this.socketChannel = connection.getSocketChannelWrapper();
        this.writeQueue = new ConcurrentLinkedQueue<OutboundFrame>();
        this.urgentWriteQueue = new ConcurrentLinkedQueue<OutboundFrame>();
        // sensors
        metricsRegistry.scanAndRegister(this, "tcp.connection[" + connection.getMetricsId() + "]");
    }

    @Override
    public void offer(OutboundFrame frame) {
        if (frame.isUrgent()) {
            urgentWriteQueue.add(frame);
        } else {
            writeQueue.add(frame);
        }
    }

    @Probe(name = "out.writeQueuePendingBytes")
    public long bytesPending() {
        return bytesPending(writeQueue);
    }

    @Probe(name = "out.priorityWriteQueuePendingBytes")
    public long priorityBytesPending() {
        return bytesPending(urgentWriteQueue);
    }

    @Probe(name = "out.idleTimeMs")
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Override
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

    @Override
    public long getLastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Override
    public WriteHandler getWriteHandler() {
        return writeHandler;
    }

    // accessed from ReadHandler and SocketConnector
    @Override
    public void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        urgentWriteQueue.add(new TaskFrame() {
            @Override
            public void run() {
                logger.info("Setting protocol: " + protocol);
                createWriter(protocol);
                latch.countDown();
            }
        });

        try {
            latch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    private void createWriter(String protocol) {
        if (writeHandler != null) {
            return;
        }

        if (CLUSTER.equals(protocol)) {
            configureBuffers(ioService.getSocketSendBufferSize() * KILO_BYTE);
            writeHandler = ioService.createWriteHandler(connection);
            outputBuffer.put(stringToBytes(CLUSTER));
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

    private void configureBuffers(int size) {
        outputBuffer = ByteBuffer.allocate(size);
        try {
            connection.setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + connection + " to " + size + " B.", e);
        }
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

    @Override
    public void start() {
        //no-op
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

    public void write() throws Exception {
        if (!connection.isAlive()) {
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

            // Lets write the currentFrame to the outputBuffer.
            if (!writeHandler.onWrite(currentFrame, outputBuffer)) {
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

    private abstract class TaskFrame implements OutboundFrame {

        abstract void run();

        @Override
        public boolean isUrgent() {
            return true;
        }
    }

    private class ShutdownTask extends TaskFrame {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        void run() {
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
                EmptyStatement.ignore(e);
            }
        }
    }
}

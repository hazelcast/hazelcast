/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.ascii.TextByteBufferWriter;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * The writing side of the {@link TcpIpConnection}.
 * <p/>
 * The handle method is triggered by 2 kinds of threads
 * - the io thread when a write-event was send indicating that there is space
 * - user thread when it writes to the connection.
 * <p/>
 * A thread becomes scheduled by a user-thread
 * A thread becomes unscheduled by the io thread when all data is written and there are no pending packets.
 */
public final class TcpIpConnectionWriteHandler extends AbstractIOEventHandler implements Runnable {

    private static final long TIMEOUT_SECONDS = 3;

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>();
    private final Queue<SocketWritable> urgentWriteQueue = new ConcurrentLinkedQueue<SocketWritable>();
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final ByteBuffer writeBuffer;
    private final IOReactor ioReactor;
    private final Selector selector;
    // Will onl be touched by single IO thread.
    private SocketWritable currentPacket;
    private ByteBufferWriter byteBufferWriter;

    // This field is written by single IO thread, but read by other threads.
    private volatile long lastWriteTime;

    TcpIpConnectionWriteHandler(TcpIpConnection connection, IOReactor ioReactor) {
        super(connection);
        this.ioReactor = ioReactor;
        this.selector = ioReactor.getSelector();
        this.writeBuffer = ByteBuffer.allocate(connectionManager.socketSendBufferSize);
    }

    long lastWriteTime() {
        return lastWriteTime;
    }

    // accessed from TcpIpConnectionReadHandler and SocketConnector
    void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        ioReactor.addTask(new Runnable() {
            public void run() {
                createWriter(protocol);
                latch.countDown();
            }
        });
        ioReactor.wakeup();
        try {
            latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // why don't we make use of the regular logger.
            Logger.getLogger(TcpIpConnectionWriteHandler.class).finest("CountDownLatch::await interrupted", e);
        }
    }

    private void createWriter(String protocol) {
        if (byteBufferWriter != null) {
            return;
        }

        if (Protocols.CLUSTER.equals(protocol)) {
            TcpIpConnectionManager connectionManager = connection.getConnectionManager();
            byteBufferWriter = new PacketByteBufferWriter(connectionManager.createPacketWriter(connection));
            writeBuffer.put(stringToBytes(Protocols.CLUSTER));
            registerOp(selector, SelectionKey.OP_WRITE);
        } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
            byteBufferWriter = new ClientByteBufferWriter();
        } else {
            byteBufferWriter = new TextByteBufferWriter(connection);
        }
    }

    public ByteBufferWriter getByteBufferWriter() {
        return byteBufferWriter;
    }

    public void enqueue(SocketWritable packet) {
        if (packet.isUrgent()) {
            urgentWriteQueue.offer(packet);
        } else {
            writeQueue.offer(packet);
        }

        schedule();
    }

    private void schedule() {
        if (scheduled.get()) {
            // So this TcpIpConnectionWriteHandler is still scheduled, we don't need to schedule it again
            return;
        }

        if (!scheduled.compareAndSet(false, true)) {
            // Another thread already has scheduled this TcpIpConnectionWriteHandler, we are done. It
            // doesn't matter which thread does the scheduling, as long as it happens.
            return;
        }

        // We managed to schedule this TcpIpConnectionWriteHandler. This means we need to add a task to
        // the ioReactor and to give the reactor-thread a kick so that it processes our packets.
        ioReactor.addTask(this);
        ioReactor.wakeup();
    }

    /**
     * Tries to unschedule this TcpIpConnectionWriteHandler.
     * <p/>
     * If
     */
    private void unschedule() {
        if (!writeBufferIsEmpty()) {
            // If the writeBuffer is not empty, we don't need to unschedule ourselves. This is because the
            // TcpIpConnectionWriteHandle will be triggered by a nio write event to continue sending data.
            return;
        }

        // So the write-buffer is empty, so we are going to unschedule ourselves.
        scheduled.set(false);

        if (writeQueue.isEmpty() && urgentWriteQueue.isEmpty()) {
            return;
        }

        // So there are packet, but we just unscheduled ourselves. If we don't try to reschedule, then these
        // Packets are at risk not to be send.

        if (!scheduled.compareAndSet(false, true)) {
            //someone else managed to schedule this TcpIpConnectionWriteHandle, so we are done.
            return;
        }

        // We managed to reschedule. So lets add ourselves to the ioReactor so we are processed again.
        // We don't need to call wakeup because the current thread is the IO-thread.
        ioReactor.addTask(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handle() {
        lastWriteTime = Clock.currentTimeMillis();

        if (!connection.isAlive()) {
            return;
        }

        if (byteBufferWriter == null) {
            logger.warning("SocketWriter is not set, creating SocketWriter with CLUSTER protocol!");
            createWriter(Protocols.CLUSTER);
        }

        try {
            fillWriteBuffer();

            if (!writeBufferIsEmpty()) {
                writeBufferToSocket();
            }
        } catch (Throwable t) {
            logger.severe("Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
        }

        unschedule();
    }

    private boolean writeBufferIsEmpty() {
        return writeBuffer.position() == 0;
    }

    /**
     * Fills the write-buffer with data. This is done by writing the currentPacket if there is one,
     * or polling new packets.
     *
     * @throws Exception
     */
    private void fillWriteBuffer() throws Exception {
//        // If there is no currentPacket, try to get one. If none is available, and the write-buffer is
//        // empty, then we are ready.
//        if (currentPacket == null) {
//            currentPacket = poll();
//            if (currentPacket == null && writeBuffer.position() == 0) {
//                return;
//            }
//        }

        for (; ; ) {
            if (!writeBuffer.hasRemaining()) {
                // The buffer is completely filled, we are done.
                return;
            }

            // If there currently is not packet sending, lets try to get one.
            if (currentPacket == null) {
                currentPacket = poll();
                if (currentPacket == null) {
                    // There is no packet to write, we are done.
                    return;
                }
            }

            // Lets write the currentPacket to the socket.
            if (!byteBufferWriter.write(currentPacket, writeBuffer)) {
                // We are done for this round because not all data of the current packet fits in the byte-buffer.
                return;
            }

            // The current packet has been written completely. So lets null it
            currentPacket = null;
        }
    }

    /**
     * Gets a packet from the urgentWriteQueue or else from the writeQueue.
     *
     * @return the retrieved packet. Null if no packet is available.
     */
    private SocketWritable poll() {
        SocketWritable writable = urgentWriteQueue.poll();
        if (writable == null) {
            writable = writeQueue.poll();
        }

        return writable;
    }

    /**
     * Writes to content of the writeBuffer to the socket.
     *
     * @throws Exception
     */
    private void writeBufferToSocket() throws Exception {
        // So there is data for writing, so lets prepare the buffer for writing and then write it to the socketChannel.
        writeBuffer.flip();
        try {
            socketChannel.write(writeBuffer);
        } catch (Exception e) {
            currentPacket = null;
            handleSocketException(e);
            return;
        }

        // Now we verify if all data is written.
        if (!writeBuffer.hasRemaining()) {
            // We managed to fully write the writeBuffer to the socket, so we are done.
            // We don't need to register for a OP_WRITE, because we have nothing left to write.
            writeBuffer.clear();
            return;
        }

        // We did not manage to write all data to the socket. So lets compact the buffer so new data
        // can be added at the end.
        writeBuffer.compact();
        // Because not all data was written to the socket, we need to register for OP_WRITE so we get
        // notified when the socketChannel is ready for more data.
        registerOp(selector, SelectionKey.OP_WRITE);
    }


    // is triggered when a packet is written to the connection.
    @Override
    public void run() {
        handle();
    }

    public void shutdown() {
        writeQueue.clear();
        urgentWriteQueue.clear();

        final CountDownLatch latch = new CountDownLatch(1);
        ioReactor.addTask(new Runnable() {
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
        });
        ioReactor.wakeup();
        try {
            latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            EmptyStatement.ignore(e);
        }
    }
}

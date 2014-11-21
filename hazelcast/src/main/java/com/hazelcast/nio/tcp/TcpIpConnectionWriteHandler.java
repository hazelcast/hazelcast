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
import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * The writing side of the {@link TcpIpConnection}.
 */
public final class TcpIpConnectionWriteHandler extends AbstractIOEventHandler implements Runnable {

    private static final long TIMEOUT_SECONDS = 3;

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>();
    private final Queue<SocketWritable> urgencyWriteQueue = new ConcurrentLinkedQueue<SocketWritable>();
    private final AtomicBoolean notifyReactor = new AtomicBoolean(true);
    private final ByteBuffer buffer;
    private final IOReactor ioReactor;
    private boolean ready;
    private SocketWritable currentPacket;
    private SocketWriter socketWriter;

    // This field is written by single IO thread, but read by other threads.
    private volatile long lastWriteTime;

    TcpIpConnectionWriteHandler(TcpIpConnection connection, IOReactor ioReactor) {
        super(connection);
        this.ioReactor = ioReactor;
        this.buffer = ByteBuffer.allocate(connectionManager.socketSendBufferSize);
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
        if (socketWriter == null) {
            if (Protocols.CLUSTER.equals(protocol)) {
                TcpIpConnectionManager connectionManager = connection.getConnectionManager();
                socketWriter = new SocketPacketWriter(connectionManager.createPacketWriter(connection));
                buffer.put(stringToBytes(Protocols.CLUSTER));
                registerWrite();
            } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
                socketWriter = new SocketClientDataWriter();
            } else {
                socketWriter = new SocketTextWriter(connection);
            }
        }
    }

    public SocketWriter getSocketWriter() {
        return socketWriter;
    }

    public void enqueue(SocketWritable packet) {
        if (packet.isUrgent()) {
            urgencyWriteQueue.offer(packet);
        } else {
            writeQueue.offer(packet);
        }

        if (notifyReactor.compareAndSet(true, false)) {
            // we don't have to call wake up if this WriteHandler already in the task queue.
            // we can have a counter to check this later on for now, wake up regardless.
            ioReactor.addTask(this);
            ioReactor.wakeup();
        }
    }

    private SocketWritable poll() {
        SocketWritable writable = urgencyWriteQueue.poll();
        if (writable == null) {
            writable = writeQueue.poll();
        }

        return writable;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handle() {
        lastWriteTime = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            return;
        }
        if (socketWriter == null) {
            logger.warning("SocketWriter is not set, creating SocketWriter with CLUSTER protocol!");
            createWriter(Protocols.CLUSTER);
        }
        if (currentPacket == null) {
            currentPacket = poll();
            if (currentPacket == null && buffer.position() == 0) {
                ready = true;
                return;
            }
        }
        try {
            writeBuffer();
        } catch (Throwable t) {
            logger.severe("Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
        } finally {
            ready = false;
            registerWrite();
        }
    }

    private void writeBuffer() throws Exception {
        while (buffer.hasRemaining() && currentPacket != null) {
            boolean complete = socketWriter.write(currentPacket, buffer);
            if (complete) {
                currentPacket = poll();
            } else {
                break;
            }
        }

        if (buffer.position() == 0) {
            return;
        }

        buffer.flip();
        try {
            socketChannel.write(buffer);
        } catch (Exception e) {
            currentPacket = null;
            handleSocketException(e);
            return;
        }

        if (buffer.hasRemaining()) {
            buffer.compact();
        } else {
            buffer.clear();
        }
    }

    @Override
    public void run() {
        notifyReactor.set(true);
        if (ready) {
            handle();
        } else {
            registerWrite();
        }
        ready = false;
    }

    private void registerWrite() {
        registerOp(ioReactor.getSelector(), SelectionKey.OP_WRITE);
    }

    public void shutdown() {
        writeQueue.clear();
        urgencyWriteQueue.clear();

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

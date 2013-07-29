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

package com.hazelcast.nio;

import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.util.Clock;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private final IOSelector ioSelector;

    private boolean ready = false;

    private SocketWritable lastWritable;

    private SocketWriter socketWriter;

    private volatile long lastHandle = 0;

    WriteHandler(TcpIpConnection connection, IOSelector ioSelector) {
        super(connection);
        this.ioSelector = ioSelector;
        buffer = ByteBuffer.allocate(connectionManager.socketSendBufferSize);
    }

    // accessed from ReadHandler
    void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        ioSelector.addTask(new Runnable() {
            public void run() {
                createWriter(protocol);
                latch.countDown();
            }
        });
        ioSelector.wakeup();
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    private void createWriter(String protocol) {
        if (socketWriter == null) {
            if (Protocols.CLUSTER.equals(protocol)) {
                socketWriter = new SocketPacketWriter(connection);
                buffer.put(Protocols.CLUSTER.getBytes());
                registerWrite();
            } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
                socketWriter = new SocketClientDataWriter(connection);
            } else {
                socketWriter = new SocketTextWriter(connection);
            }
        }
    }

    public SocketWriter getSocketWriter() {
        return socketWriter;
    }

    public void enqueueSocketWritable(SocketWritable socketWritable) {
        socketWritable.onEnqueue();
        writeQueue.offer(socketWritable);
        if (informSelector.compareAndSet(true, false)) {
            // we don't have to call wake up if this WriteHandler is
            // already in the task queue.
            // we can have a counter to check this later on.
            // for now, wake up regardless.
            ioSelector.addTask(this);
            ioSelector.wakeup();
        }
    }

    private SocketWritable poll() {
        return writeQueue.poll();
    }

    @SuppressWarnings("unchecked")
    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.live()) {
            return;
        }
        if (socketWriter == null) {
            createWriter(Protocols.CLUSTER);
        }
        if (lastWritable == null && (lastWritable = poll()) == null && buffer.position() == 0) {
            ready = true;
            return;
        }
        try {
            while (buffer.hasRemaining() && lastWritable != null) {
                boolean complete = socketWriter.write(lastWritable, buffer);
                if (complete) {
                    lastWritable = poll();
                } else {
                    break;
                }
            }
            if (buffer.position() > 0) {
                buffer.flip();
                try {
                    socketChannel.write(buffer);
                } catch (Exception e) {
                    lastWritable = null;
                    handleSocketException(e);
                    return;
                }
                if (buffer.hasRemaining()) {
                    buffer.compact();
                } else {
                    buffer.clear();
                }
            }
        } catch (Throwable t) {
            logger.severe("Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
            connection.getSystemLogService().logConnection("Fatal Error at WriteHandler for endPoint " +
                    "[" + connection.getEndPoint() + "]: " + t.getMessage());
        } finally {
            ready = false;
            registerWrite();
        }
    }

    public void run() {
        informSelector.set(true);
        if (ready) {
            handle();
        } else {
            registerWrite();
        }
        ready = false;
    }

    private void registerWrite() {
        registerOp(ioSelector.getSelector(), SelectionKey.OP_WRITE);
    }

    @Override
    public void shutdown() {
        while (poll() != null) ;
    }

    long getLastHandle() {
        return lastHandle;
    }
}

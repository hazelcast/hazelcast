/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.logging.SystemArgsLog;
import com.hazelcast.nio.ascii.SocketTextWriter;
import com.hazelcast.util.Clock;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>() {
        final AtomicInteger size = new AtomicInteger();

        @Override
        public boolean offer(SocketWritable socketWritable) {
            if (super.offer(socketWritable)) {
                size.incrementAndGet();
                return true;
            }
            return false;
        }

        @Override
        public SocketWritable poll() {
            final SocketWritable socketWritable = super.poll();
            if (socketWritable != null) {
                size.decrementAndGet();
            }
            return socketWritable;
        }

        @Override
        public int size() {
            return size.get();
        }
    };

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private boolean ready = false;

    private SocketWritable lastWritable = null;

    private volatile SocketWriter socketWriter = null; // accessed from ReadHandler

    volatile long lastRegistration = 0;

    volatile long lastHandle = 0;

    WriteHandler(Connection connection) {
        super(connection, connection.getInOutSelector());
        buffer = ByteBuffer.allocate(connectionManager.SOCKET_SEND_BUFFER_SIZE);
    }

    void setProtocol(String protocol) {
        if (socketWriter == null) {
            if ("HZC".equals(protocol)) {
                socketWriter = new SocketPacketWriter(connection);
                buffer.put("HZC".getBytes());
                inOutSelector.addTask(this);
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
            inOutSelector.addTask(this);
            inOutSelector.selector.wakeup();
        }
    }

    SocketWritable poll() {
        SocketWritable sw = writeQueue.poll();
//        if (sw instanceof Packet) {
//            Packet ssw = (Packet) sw;
//            Packet packet = connection.obtainPacket();
//            ssw.setToPacket(packet);
//            packet.onEnqueue();
//            return packet;
//        }
        return sw;
    }

    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (socketWriter == null) {
            setProtocol("HZC");
        }
        if (lastWritable == null) {
            lastWritable = poll();
            if (lastWritable == null && buffer.position() == 0) {
                ready = true;
                return;
            }
        }
        if (!connection.live())
            return;
        try {
            while (buffer.hasRemaining()) {
                if (lastWritable == null) {
                    lastWritable = poll();
                }
                if (lastWritable != null) {
                    boolean complete = socketWriter.write(lastWritable, buffer);
                    if (complete) {
                        if (lastWritable instanceof Packet) {
                            Packet packet = (Packet) lastWritable;
                            connection.releasePacket(packet);
                            if (systemLogService.shouldTrace()) {
                                systemLogService.trace(packet, new SystemArgsLog("WrittenOut ",
                                        connection.getEndPoint()/*, packet.operation*/));
                            }
                        }
                        lastWritable = null;
                    } else {
                        if (buffer.hasRemaining()) {
                            break;
                        }
                    }
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
            logger.log(Level.SEVERE, "Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
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
        lastRegistration = Clock.currentTimeMillis();
        registerOp(inOutSelector.selector, SelectionKey.OP_WRITE);
    }

    @Override
    public void shutdown() {
        while (poll() != null);
    }

    public int size() {
        return writeQueue.size();
    }
}

/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.nio.ascii.SocketTextWriter;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer socketBB;

    private boolean ready = false;

    private volatile SocketWritable lastWritable = null;

    private volatile SocketWriter socketWriter = null;

    private long runCount = 0;

    volatile long lastRegistration = 0;
    volatile long lastHandle = 0;

    WriteHandler(Connection connection) {
        super(connection, connection.inOutSelector);
        socketBB = ByteBuffer.allocate(node.connectionManager.SOCKET_SEND_BUFFER_SIZE);
    }

    public void setProtocol(String protocol) {
        if (socketWriter == null) {
            if ("HZC".equals(protocol)) {
                socketWriter = new SocketPacketWriter(node, connection);
                socketBB.put("HZC".getBytes());
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
        inOutSelector.writeQueueSize.incrementAndGet();
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
        if (sw != null) {
            inOutSelector.writeQueueSize.decrementAndGet();
        }
        return sw;
    }

    public void handle() {
        lastHandle = System.currentTimeMillis();
        runCount++;
        if (socketWriter == null) {
            setProtocol("HZC");
        }
        if (lastWritable == null) {
            lastWritable = poll();
            if (lastWritable == null && socketBB.position() == 0) {
                ready = true;
                return;
            }
        }
        if (!connection.live())
            return;
        try {
            while (socketBB.hasRemaining()) {
                if (lastWritable == null) {
                    lastWritable = poll();
                }
                if (lastWritable != null) {
                    boolean complete = socketWriter.write(lastWritable, socketBB);
                    if (complete) {
                        if (lastWritable instanceof Packet) {
                            connection.releasePacket((Packet) lastWritable);
                        }
                        lastWritable = null;
                    } else {
                        if (socketBB.hasRemaining()) {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            if (socketBB.position() > 0) {
                socketBB.flip();
                try {
                    int written = socketChannel.write(socketBB);
                } catch (Exception e) {
                    lastWritable = null;
                    handleSocketException(e);
                    return;
                }
                if (socketBB.hasRemaining()) {
                    socketBB.compact();
                } else {
                    socketBB.clear();
                }
            }
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
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
        lastRegistration = System.currentTimeMillis();
        registerOp(inOutSelector.selector, SelectionKey.OP_WRITE);
    }

    @Override
    public void shutdown() {
        Object obj = poll();
        while (obj != null) {
            obj = poll();
        }
    }

    public int size() {
        return writeQueue.size();
    }
}

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

package com.hazelcast.client.connection.nio;

import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.util.Clock;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientWriteHandler extends AbstractClientSelectionHandler implements Runnable {

    private final Queue<SocketWritable> writeQueue = new ConcurrentLinkedQueue<SocketWritable>();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private boolean ready;

    private SocketWritable lastWritable;

    private volatile long lastHandle;

    public ClientWriteHandler(ClientConnection connection, IOSelector ioSelector, int bufferSize) {
        super(connection, ioSelector);
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            return;
        }

        if (lastWritable == null) {
            lastWritable = poll();
        }

        if (lastWritable == null && buffer.position() == 0) {
            ready = true;
            return;
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

    private void writeBuffer() {
        while (buffer.hasRemaining() && lastWritable != null) {
            boolean complete = lastWritable.writeTo(buffer);
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
    }

    public void enqueueSocketWritable(SocketWritable socketWritable) {
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

    @Override
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
        registerOp(SelectionKey.OP_WRITE);
    }

    @Override
    public void shutdown() {
        writeQueue.clear();
    }

    long getLastHandle() {
        return lastHandle;
    }
}

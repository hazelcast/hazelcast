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

package com.hazelcast.client.connection.nio;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Frame;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientWriteHandler extends AbstractClientSelectionHandler implements Runnable {

    private final Queue<Packet> writeQueue = new ConcurrentLinkedQueue<Packet>();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private boolean ready;

    private Packet lastPacket;

    private volatile long lastHandle;

    public ClientWriteHandler(ClientConnection connection, NonBlockingIOThread ioThread, int bufferSize) {
        super(connection, ioThread);
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void handle() throws IOException {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            return;
        }

        if (lastPacket == null) {
            lastPacket = poll();
        }

        if (lastPacket == null && buffer.position() == 0) {
            ready = true;
            return;
        }

        writeBuffer();
        ready = false;
        registerWrite();
    }

    private void writeBuffer() throws IOException {
        while (buffer.hasRemaining() && lastPacket != null) {
            boolean complete = lastPacket.writeTo(buffer);
            if (complete) {
                lastPacket = poll();
            } else {
                break;
            }
        }
        if (buffer.position() > 0) {
            buffer.flip();
            socketChannel.write(buffer);

            if (buffer.hasRemaining()) {
                buffer.compact();
            } else {
                buffer.clear();
            }
        }
    }

    public void enqueue(Frame frame) {
        writeQueue.offer((Packet) frame);
        if (informSelector.compareAndSet(true, false)) {
            // we don't have to call wake up if this WriteHandler is
            // already in the task queue.
            // we can have a counter to check this later on.
            // for now, wake up regardless.
            register();
        }
    }

    private Packet poll() {
        return writeQueue.poll();
    }

    @Override
    public void run() {
        try {
            informSelector.set(true);
            if (ready) {
                handle();
            } else {
                registerWrite();
            }
            ready = false;
        } catch (Throwable t) {
            onFailure(t);
        }
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

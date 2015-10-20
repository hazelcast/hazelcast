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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientWriteHandler extends AbstractClientSelectionHandler implements Runnable {

    private final Queue<ClientMessage> writeQueue = new ConcurrentLinkedQueue<ClientMessage>();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private boolean ready;

    private ClientMessage lastMessage;

    private volatile long lastHandle;

    public ClientWriteHandler(ClientConnection connection, NonBlockingIOThread ioThread, int bufferSize) {
        super(connection, ioThread);
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void handle() throws Exception {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            return;
        }

        if (lastMessage == null) {
            lastMessage = poll();
        }

        if (lastMessage == null && buffer.position() == 0) {
            ready = true;
            return;
        }

        writeBuffer();
        ready = false;
        registerWrite();
    }

    private void writeBuffer() throws IOException {
        while (buffer.hasRemaining() && lastMessage != null) {
            boolean complete = lastMessage.writeTo(buffer);
            if (complete) {
                lastMessage = poll();
            } else {
                break;
            }
        }

        if (buffer.position() == 0) {
            // there is nothing to write, we are done
            return;
        }

        buffer.flip();
        socketChannel.write(buffer);

        if (buffer.hasRemaining()) {
            buffer.compact();
        } else {
            buffer.clear();
        }
    }

    public void enqueue(OutboundFrame frame) {
        writeQueue.offer((ClientMessage) frame);
        if (informSelector.compareAndSet(true, false)) {
            // we don't have to call wake up if this WriteHandler is
            // already in the task queue.
            // we can have a counter to check this later on.
            // for now, wake up regardless.
            register();
        }
    }

    private ClientMessage poll() {
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

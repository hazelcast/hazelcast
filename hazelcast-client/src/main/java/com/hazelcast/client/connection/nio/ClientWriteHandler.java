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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

/**
 * ClientReadHandler gets called by an IO-thread when there is space available to write to.
 * It then writes some of its enqueued data to the socket from a bytebuffer.
 */
public class ClientWriteHandler extends AbstractClientSelectionHandler implements Runnable {

    @Probe(name = "writeQueueSize")
    private final Queue<ClientMessage> writeQueue = new ConcurrentLinkedQueue<ClientMessage>();
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = "messagesWritten")
    private final SwCounter messagesWritten = newSwCounter();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer buffer;

    private boolean ready;

    private ClientMessage lastMessage;

    private volatile long lastHandle;

    public ClientWriteHandler(ClientConnection connection, NonBlockingIOThread ioThread, int bufferSize,
                              boolean direct, LoggingService loggingService) {
        super(connection, ioThread, loggingService);
        buffer = IOUtil.newByteBuffer(bufferSize, direct);
    }

    @Probe(name = "idleTimeMs", level = DEBUG)
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastHandle, 0);
    }

    @Probe(name = "isScheduled", level = DEBUG)
    private long isScheduled() {
        return informSelector.get() ? 0 : 1;
    }

    @Override
    public void handle() throws Exception {
        eventCount.inc();

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
        bytesWritten.inc(socketChannel.write(buffer));

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
        ClientMessage message = writeQueue.poll();
        if (message != null) {
            messagesWritten.inc();
        }
        return message;
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

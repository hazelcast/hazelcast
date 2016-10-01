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
import com.hazelcast.client.impl.protocol.util.ClientMessageReadHandler;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.util.Clock;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

/**
 * ClientNonBlockingSocketReader gets called by an IO-thread when there is data available to read.
 * It then reads out the data from the socket into a bytebuffer and hands it over to the {@link ClientMessageReadHandler}
 * to get processed.
 */
public class ClientNonBlockingSocketReader
        extends AbstractClientHandler {

    private final ByteBuffer inputBuffer;
    private final ReadHandler readHandler;
    @Probe(name = "messagesRead")
    private final SwCounter messagesRead = newSwCounter();
    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    private volatile long lastReadTime;

    public ClientNonBlockingSocketReader(final ClientConnection connection,
                                         NonBlockingIOThread ioThread,
                                         int bufferSize,
                                         boolean direct,
                                         LoggingService loggingService) {
        super(connection, ioThread, loggingService, SelectionKey.OP_READ);

        this.inputBuffer = IOUtil.newByteBuffer(bufferSize, direct);
        this.lastReadTime = Clock.currentTimeMillis();
        this.readHandler = new ClientMessageReadHandler(messagesRead, new ClientMessageReadHandler.MessageHandler() {
            @Override
            public void handleMessage(ClientMessage message) {
                connectionManager.handleClientMessage(message, connection);
            }
        });
    }

    @Probe(level = DEBUG)
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastReadTime, 0);
    }

    long getLastReadTime() {
        return lastReadTime;
    }

    @Override
    public void handle() throws Exception {
        eventCount.inc();

        lastReadTime = Clock.currentTimeMillis();

        int readBytes = socketChannel.read(inputBuffer);
        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }
        bytesRead.inc(readBytes);

        inputBuffer.flip();

        readHandler.onRead(inputBuffer);

        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
    }
}

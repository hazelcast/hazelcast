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
import com.hazelcast.client.impl.protocol.util.ClientMessageBuilder;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.util.Clock;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class ClientReadHandler
        extends AbstractClientSelectionHandler {

    private final ByteBuffer buffer;
    private final ClientMessageBuilder builder;

    private volatile long lastHandle;

    public ClientReadHandler(final ClientConnection connection, NonBlockingIOThread ioThread, int bufferSize,
                             boolean direct, LoggingService loggingService) {
        super(connection, ioThread, loggingService);

        buffer = IOUtil.newByteBuffer(bufferSize, direct);
        lastHandle = Clock.currentTimeMillis();
        builder = new ClientMessageBuilder(new ClientMessageBuilder.MessageHandler() {
            @Override
            public void handleMessage(ClientMessage message) {
                connectionManager.handleClientMessage(message, connection);
            }
        });
    }

    @Override
    public void run() {
        registerOp(SelectionKey.OP_READ);
    }

    @Override
    public void handle() throws Exception {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            if (logger.isFinestEnabled()) {
                logger.finest("We are being asked to read, but connection is not live so we won't");
            }
            return;
        }

        int readBytes = socketChannel.read(buffer);
        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }

        buffer.flip();

        builder.onData(buffer);

        if (buffer.hasRemaining()) {
            buffer.compact();
        } else {
            buffer.clear();
        }
    }

    long getLastHandle() {
        return lastHandle;
    }

}

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

import com.hazelcast.nio.ClientPacket;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.util.Clock;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class ClientReadHandler extends ClientAbstractSelectionHandler {

    private final ByteBuffer buffer;

    private volatile long lastHandle;

    private ClientPacket packet;

    public ClientReadHandler(ClientConnection connection, IOSelector ioSelector, int bufferSize) {
        super(connection, ioSelector);
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override

    public void run() {
        registerOp(SelectionKey.OP_READ);
    }

    @Override
    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.live()) {
            if (logger.isFinestEnabled()) {
                String message = "We are being asked to read, but connection is not live so we won't";
                logger.finest(message);
            }
            return;
        }
        try {
            int readBytes = socketChannel.read(buffer);
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
        } catch (IOException e) {
            handleSocketException(e);
            return;
        }
        try {
            if (buffer.position() == 0) {
                return;
            }
            buffer.flip();

            while (buffer.hasRemaining()) {
                if (packet == null) {
                    final SerializationService ss = connection.getConnectionManager().getSerializationService();
                    packet = new ClientPacket(ss.getPortableContext());
                }
                boolean complete = packet.readFrom(buffer);
                if (complete) {
                    packet.setConn(connection);
                    connectionManager.handlePacket(packet);
                    packet = null;
                } else {
                    break;
                }
            }

            if (buffer.hasRemaining()) {
                buffer.compact();
            } else {
                buffer.clear();
            }
        } catch (Throwable t) {
            handleSocketException(t);
        }

    }

    long getLastHandle() {
        return lastHandle;
    }

}

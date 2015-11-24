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
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.util.Clock;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class ClientReadHandler extends AbstractClientSelectionHandler {

    private final ByteBuffer buffer;

    private volatile long lastHandle;

    private Packet packet;

    public ClientReadHandler(ClientConnection connection, NonBlockingIOThread ioThread, int bufferSize) {
        super(connection, ioThread);
        buffer = ByteBuffer.allocate(bufferSize);
        lastHandle = Clock.currentTimeMillis();
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

        readPacket();

        if (buffer.hasRemaining()) {
            buffer.compact();
        } else {
            buffer.clear();
        }
    }

    private void readPacket() {
        while (buffer.hasRemaining()) {
            if (packet == null) {
                packet = new Packet();
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
    }

    long getLastHandle() {
        return lastHandle;
    }

}

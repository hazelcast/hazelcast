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

package com.hazelcast.nio;

import com.hazelcast.nio.ascii.SocketTextReader;
import com.hazelcast.util.Clock;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.util.StringUtil.bytesToString;

/**
 * The reading side of the {@link com.hazelcast.nio.Connection}.
 */
final class ReadHandler extends AbstractSelectionHandler implements Runnable {

    private final ByteBuffer buffer;

    private final IOSelector ioSelector;

    private SocketReader socketReader;

    private volatile long lastHandle;

    public ReadHandler(TcpIpConnection connection, IOSelector ioSelector) {
        super(connection);
        this.ioSelector = ioSelector;
        buffer = ByteBuffer.allocate(connectionManager.socketReceiveBufferSize);
    }

    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.live()) {
            String message = "We are being asked to read, but connection is not live so we won't";
            logger.finest(message);
            systemLogService.logConnection(message);
            return;
        }
        try {
            if (socketReader == null) {
                initializeSocketReader();
                if (socketReader == null) {
                    // when using SSL, we can read 0 bytes since data read from socket can be handshake packets.
                    return;
                }
            }
            int readBytes = socketChannel.read(buffer);
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
        } catch (Throwable e) {
            handleSocketException(e);
            return;
        }
        try {
            if (buffer.position() == 0) {
                return;
            }
            buffer.flip();
            socketReader.read(buffer);
            if (buffer.hasRemaining()) {
                buffer.compact();
            } else {
                buffer.clear();
            }
        } catch (Throwable t) {
            handleSocketException(t);
        }
    }

    private void initializeSocketReader() throws IOException {
        if (socketReader == null) {
            final ByteBuffer protocolBuffer = ByteBuffer.allocate(3);
            int readBytes = socketChannel.read(protocolBuffer);
            if (readBytes == -1) {
                throw new EOFException("Could not read protocol type!");
            }
            if (readBytes == 0 && connectionManager.isSSLEnabled()) {
                // when using SSL, we can read 0 bytes since data read from socket can be handshake packets.
                return;
            }
            if (!protocolBuffer.hasRemaining()) {
                String protocol = bytesToString(protocolBuffer.array());
                WriteHandler writeHandler = connection.getWriteHandler();
                if (Protocols.CLUSTER.equals(protocol)) {
                    connection.setType(ConnectionType.MEMBER);
                    writeHandler.setProtocol(Protocols.CLUSTER);
                    socketReader = new SocketPacketReader(connection);
                } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
                    writeHandler.setProtocol(Protocols.CLIENT_BINARY);
                    socketReader = new SocketClientDataReader(connection);
                } else {
                    writeHandler.setProtocol(Protocols.TEXT);
                    buffer.put(protocolBuffer.array());
                    socketReader = new SocketTextReader(connection);
                    connection.getConnectionManager().incrementTextConnections();
                }
            }
            if (socketReader == null) {
                throw new IOException("Could not initialize SocketReader!");
            }
        }
    }

    public void run() {
        registerOp(ioSelector.getSelector(), SelectionKey.OP_READ);
    }

    long getLastHandle() {
        return lastHandle;
    }

    public void register() {
        ioSelector.addTask(this);
        ioSelector.wakeup();
    }
}

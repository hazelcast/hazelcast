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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Protocols;
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
final class ReadHandler
        extends AbstractSelectionHandler {

    private final ByteBuffer inputBuffer;

    private final IOSelector ioSelector;

    private SocketReader socketReader;

    private volatile long lastHandle;

    public ReadHandler(TcpIpConnection connection, IOSelector ioSelector) {
        super(connection, ioSelector, SelectionKey.OP_READ);
        this.ioSelector = ioSelector;
        this.inputBuffer = ByteBuffer.allocate(connectionManager.socketReceiveBufferSize);
    }

    public void start() {
        ioSelector.addTask(new Runnable() {
            @Override
            public void run() {
                getSelectionKey();

            }
        });

        ioSelector.wakeup();
    }

    @Override
    public void handle() {
        lastHandle = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            String message = "We are being asked to read, but connection is not live so we won't";
            logger.finest(message);
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
            int readBytes = socketChannel.read(inputBuffer);
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
        } catch (Throwable e) {
            handleSocketException(e);
            return;
        }
        try {
            if (inputBuffer.position() == 0) {
                return;
            }
            inputBuffer.flip();
            socketReader.read(inputBuffer);
            if (inputBuffer.hasRemaining()) {
                inputBuffer.compact();
            } else {
                inputBuffer.clear();
            }
        } catch (Throwable t) {
            handleSocketException(t);
        }
    }

    private void initializeSocketReader()
            throws IOException {
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
                } else if (Protocols.CLIENT_BINARY_NEW.equals(protocol)) {
                    setupClient(writeHandler);
                } else {
                    writeHandler.setProtocol(Protocols.TEXT);
                    inputBuffer.put(protocolBuffer.array());
                    socketReader = new SocketTextReader(connection);
                    connection.getConnectionManager().incrementTextConnections();
                }
            }
            if (socketReader == null) {
                throw new IOException("Could not initialize SocketReader!");
            }
        }
    }

    private void setupClient(WriteHandler writeHandler)
            throws IOException {
        final ByteBuffer clientTypeBuffer = ByteBuffer.allocate(3);
        int size;
        do {
            size = socketChannel.read(clientTypeBuffer);
        } while (size < 3);
        clientTypeBuffer.flip();
        SocketClientDataReaderNew reader = new SocketClientDataReaderNew(connection);
        if (reader.setConnectionType(clientTypeBuffer)) {
            socketReader = reader;
            writeHandler.setProtocol(Protocols.CLIENT_BINARY);
        }
    }

    long getLastHandle() {
        return lastHandle;
    }

    void shutdown() {
        ioSelector.addTask(new Runnable() {
            @Override
            public void run() {
                try {
                    socketChannel.closeInbound();
                } catch (IOException e) {
                    logger.finest("Error while closing inbound", e);
                }
            }
        });
        ioSelector.wakeup();
    }
}

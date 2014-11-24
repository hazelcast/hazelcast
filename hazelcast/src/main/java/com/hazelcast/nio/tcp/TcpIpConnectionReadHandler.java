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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.ascii.TextByteBufferReader;
import com.hazelcast.util.Clock;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.util.StringUtil.bytesToString;

/**
 * The reading side of the {@link com.hazelcast.nio.Connection}.
 */
final class TcpIpConnectionReadHandler extends AbstractIOEventHandler implements Runnable {

    private final ByteBuffer buffer;
    private final IOReactor ioReactor;
    private ByteBufferReader byteBufferReader;

    // This field is written by single IO thread, but read by other threads.
    private volatile long lastReadTime;

    public TcpIpConnectionReadHandler(TcpIpConnection connection, IOReactor ioReactor) {
        super(connection);
        this.ioReactor = ioReactor;
        this.buffer = ByteBuffer.allocate(connectionManager.socketReceiveBufferSize);
    }

    @Override
    public void handle() {
        lastReadTime = Clock.currentTimeMillis();
        if (!connection.isAlive()) {
            logger.finest("We are being asked to read, but connection is not live so we won't");
            return;
        }

        try {
            if (byteBufferReader == null) {
                initializeSocketReader();
                if (byteBufferReader == null) {
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
            byteBufferReader.read(buffer);
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
        if (byteBufferReader == null) {
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
                TcpIpConnectionWriteHandler writeHandler = connection.getWriteHandler();
                if (Protocols.CLUSTER.equals(protocol)) {
                    connection.setType(ConnectionType.MEMBER);
                    writeHandler.setProtocol(Protocols.CLUSTER);
                    TcpIpConnectionManager connectionManager = connection.getConnectionManager();
                    byteBufferReader = new PacketByteBufferReader(connectionManager.createPacketReader(connection));
                } else if (Protocols.CLIENT_BINARY.equals(protocol)) {
                    writeHandler.setProtocol(Protocols.CLIENT_BINARY);
                    byteBufferReader = new ClientByteBufferReader(connection);
                } else {
                    writeHandler.setProtocol(Protocols.TEXT);
                    buffer.put(protocolBuffer.array());
                    byteBufferReader = new TextByteBufferReader(connection);
                    connection.getConnectionManager().incrementTextConnections();
                }
            }
            if (byteBufferReader == null) {
                throw new IOException("Could not initialize SocketReader!");
            }
        }
    }

    @Override
    public void run() {
        registerOp(ioReactor.getSelector(), SelectionKey.OP_READ);
    }

    /**
     * Returns the time in ms when this ReadHandler handled a packet for the last time.
     *
     * @return
     */
    long lastReadTime() {
        return lastReadTime;
    }

    public void register() {
        ioReactor.addTask(this);
        ioReactor.wakeup();
    }

    void shutdown() {
        ioReactor.addTask(new Runnable() {
            @Override
            public void run() {
                try {
                    socketChannel.closeInbound();
                } catch (IOException e) {
                    logger.finest("Error while closing inbound", e);
                }
            }
        });
        ioReactor.wakeup();
    }
}

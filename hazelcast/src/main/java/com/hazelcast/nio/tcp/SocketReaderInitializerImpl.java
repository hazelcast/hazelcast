/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketReader;
import com.hazelcast.internal.networking.SocketReaderInitializer;
import com.hazelcast.internal.networking.SocketWriter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.ascii.TextReadHandler;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.ConnectionType.MEMBER;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.nio.Protocols.TEXT;
import static com.hazelcast.util.StringUtil.bytesToString;

public class SocketReaderInitializerImpl implements SocketReaderInitializer<TcpIpConnection> {

    private final ILogger logger;

    public SocketReaderInitializerImpl(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public void init(TcpIpConnection connection, SocketReader reader) throws IOException {
        TcpIpConnectionManager connectionManager = connection.getConnectionManager();
        IOService ioService = connectionManager.getIoService();

        ByteBuffer protocolBuffer = reader.getProtocolBuffer();
        SocketChannelWrapper socketChannel = reader.getSocketChannel();

        int readBytes = socketChannel.read(protocolBuffer);

        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (readBytes == 0 && connectionManager.isSSLEnabled()) {
            // when using SSL, we can read 0 bytes since data read from socket can be handshake frames.
            return;
        }

        if (protocolBuffer.hasRemaining()) {
            // we have not yet received all protocol bytes
            return;
        }

        ReadHandler readHandler;
        String protocol = bytesToString(protocolBuffer.array());
        SocketWriter socketWriter = connection.getSocketWriter();
        if (CLUSTER.equals(protocol)) {
            initInputBuffer(connection, reader, ioService.getSocketReceiveBufferSize());
            connection.setType(MEMBER);
            socketWriter.setProtocol(CLUSTER);
            readHandler = ioService.createReadHandler(connection);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            initInputBuffer(connection, reader, ioService.getSocketClientReceiveBufferSize());
            socketWriter.setProtocol(CLIENT_BINARY_NEW);
            readHandler = new ClientReadHandler(reader.getNormalFramesReadCounter(), connection, ioService);
        } else {
            ByteBuffer inputBuffer = initInputBuffer(connection, reader, ioService.getSocketReceiveBufferSize());
            socketWriter.setProtocol(TEXT);
            inputBuffer.put(protocolBuffer.array());
            readHandler = new TextReadHandler(connection);
            connectionManager.incrementTextConnections();
        }

        if (readHandler == null) {
            throw new IOException("Could not initialize ReadHandler!");
        }

        reader.initReadHandler(readHandler);
    }

    private ByteBuffer initInputBuffer(TcpIpConnection connection, SocketReader reader, int sizeKb) {
        boolean directBuffer = connection.getConnectionManager().getIoService().isSocketBufferDirect();
        int sizeBytes = sizeKb * KILO_BYTE;

        ByteBuffer inputBuffer = newByteBuffer(sizeBytes, directBuffer);
        reader.initInputBuffer(inputBuffer);

        try {
            connection.setReceiveBufferSize(sizeBytes);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + connection + " to " + sizeBytes + " B.", e);
        }

        return inputBuffer;
    }
}

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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.ascii.TextReadHandler;
import com.hazelcast.nio.tcp.ClientReadHandler;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.ConnectionType.MEMBER;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.bytesToString;

/**
 * A {@link SocketReader} tailored for non blocking IO.
 *
 * When the {@link NonBlockingIOThread} receives a read event from the {@link java.nio.channels.Selector}, then the
 * {@link #handle()} is called to read out the data from the socket into a bytebuffer and hand it over to the
 * {@link ReadHandler} to get processed.
 */
public final class NonBlockingSocketReader
        extends AbstractNonBlockingSocketReader<TcpIpConnection> {

    private final TcpIpConnectionManager connectionManager;
    private final IOService ioService;

    public NonBlockingSocketReader(
            TcpIpConnection connection,
            NonBlockingIOThread ioThread,
            ILogger logger,
            IOBalancer balancer) {
        super(connection, connection.getSocketChannelWrapper(), ioThread, logger, balancer);

        this.connectionManager = connection.getConnectionManager();
        this.ioService = connectionManager.getIoService();
    }

    @Override
    protected ReadHandler initReadHandler() throws IOException {
        ByteBuffer protocolBuffer = ByteBuffer.allocate(3);
        int readBytes = socketChannel.read(protocolBuffer);
        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (readBytes == 0 && connectionManager.isSSLEnabled()) {
            // when using SSL, we can read 0 bytes since data read from socket can be handshake frames.
            return null;
        }

        ReadHandler readHandler = null;
        if (!protocolBuffer.hasRemaining()) {
            String protocol = bytesToString(protocolBuffer.array());
            SocketWriter socketWriter = connection.getSocketWriter();
            if (CLUSTER.equals(protocol)) {
                configureBuffers(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
                connection.setType(MEMBER);
                socketWriter.setProtocol(CLUSTER);
                readHandler = ioService.createReadHandler(connection);
            } else if (CLIENT_BINARY_NEW.equals(protocol)) {
                configureBuffers(ioService.getSocketClientReceiveBufferSize() * KILO_BYTE);
                socketWriter.setProtocol(CLIENT_BINARY_NEW);
                readHandler = new ClientReadHandler(getNormalFramesReadCounter(), connection, ioService);
            } else {
                configureBuffers(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
                socketWriter.setProtocol(Protocols.TEXT);
                inputBuffer.put(protocolBuffer.array());
                readHandler = new TextReadHandler(connection);
                connection.getConnectionManager().incrementTextConnections();
            }
        }

        if (readHandler == null) {
            throw new IOException("Could not initialize ReadHandler!");
        }
        return readHandler;
    }

    private void configureBuffers(int size) {
        super.configureInputBuffer(size, ioService.isSocketBufferDirect());

        try {
            connection.setReceiveBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + connection + " to " + size + " B.", e);
        }
    }
}

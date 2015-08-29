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

package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.ascii.TextReadHandler;
import com.hazelcast.nio.tcp.NewClientReadHandler;
import com.hazelcast.nio.tcp.OldClientReadHandler;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.util.counters.Counter;
import com.hazelcast.util.counters.SwCounter;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.ConnectionType.MEMBER;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class SpinningSocketReader extends AbstractHandler implements SocketReader {

    @Probe(name = "in.bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "in.normalPacketsRead")
    private final SwCounter normalPacketsRead = newSwCounter();
    @Probe(name = "in.priorityPacketsRead")
    private final SwCounter priorityPacketsRead = newSwCounter();
    private final MetricsRegistry metricRegistry;
    private final SocketChannelWrapper socketChannel;
    private volatile long lastReadTime;
    private ReadHandler readHandler;
    private ByteBuffer inputBuffer;
    private ByteBuffer protocolBuffer = ByteBuffer.allocate(3);

    public SpinningSocketReader(TcpIpConnection connection, MetricsRegistry metricsRegistry, ILogger logger) {
        super(connection, logger);
        this.metricRegistry = metricsRegistry;
        this.socketChannel = connection.getSocketChannelWrapper();
        metricRegistry.scanAndRegister(this, "tcp.connection[" + connection.getMetricsId() + "]");
    }

    @Override
    public long getLastReadTimeMillis() {
        return lastReadTime;
    }

    @Probe(name = "in.idleTimeMs")
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastReadTime, 0);
    }

    @Override
    public Counter getNormalPacketsReadCounter() {
        return normalPacketsRead;
    }

    @Override
    public Counter getPriorityPacketsReadCounter() {
        return priorityPacketsRead;
    }

    @Override
    public void init() {
        //no-op
    }

    @Override
    public void destroy() {
        metricRegistry.deregister(this);
    }

    public void read() throws Exception {
        if (!connection.isAlive()) {
            socketChannel.closeInbound();
            return;
        }

        if (readHandler == null) {
            initializeSocketReader();
            if (readHandler == null) {
                // when using SSL, we can read 0 bytes since data read from socket can be handshake packets.
                return;
            }
        }

        int readBytes = socketChannel.read(inputBuffer);
        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }

        lastReadTime = currentTimeMillis();
        bytesRead.inc(readBytes);
        inputBuffer.flip();
        readHandler.onRead(inputBuffer);
        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
    }

    private void initializeSocketReader() throws IOException {
        if (readHandler != null) {
            return;
        }

        int readBytes = socketChannel.read(protocolBuffer);
        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (readBytes == 0 && connectionManager.isSSLEnabled()) {
            // when using SSL, we can read 0 bytes since data read from socket can be handshake packets.
            return;
        }

        if (protocolBuffer.hasRemaining()) {
            // we have not yet received all protocol bytes
            return;
        }

        String protocol = bytesToString(protocolBuffer.array());
        SocketWriter socketWriter = connection.getSocketWriter();
        if (CLUSTER.equals(protocol)) {
            configureBuffers(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
            connection.setType(MEMBER);
            socketWriter.setProtocol(CLUSTER);
            readHandler = ioService.createReadHandler(connection);
        } else if (CLIENT_BINARY.equals(protocol)) {
            configureBuffers(ioService.getSocketClientReceiveBufferSize() * KILO_BYTE);
            socketWriter.setProtocol(CLIENT_BINARY);
            readHandler = new OldClientReadHandler(connection, ioService);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            configureBuffers(ioService.getSocketClientReceiveBufferSize() * KILO_BYTE);
            socketWriter.setProtocol(CLIENT_BINARY_NEW);
            readHandler = new NewClientReadHandler(connection, ioService);
        } else {
            configureBuffers(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
            socketWriter.setProtocol(Protocols.TEXT);
            inputBuffer.put(protocolBuffer.array());
            readHandler = new TextReadHandler(connection);
            connection.getConnectionManager().incrementTextConnections();
        }
    }

    private void configureBuffers(int size) {
        inputBuffer = ByteBuffer.allocate(size);
        try {
            connection.setReceiveBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + connection + " to " + size + " B.", e);
        }
    }
}

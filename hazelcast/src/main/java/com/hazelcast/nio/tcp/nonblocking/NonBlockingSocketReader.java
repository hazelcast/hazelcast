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

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.ascii.TextReadHandler;
import com.hazelcast.nio.tcp.ClientReadHandler;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.SocketWriter;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.ConnectionType.MEMBER;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.bytesToString;
import static java.lang.System.currentTimeMillis;

/**
 * A {@link SocketReader} tailored for non blocking IO.
 *
 * When the {@link NonBlockingIOThread} receives a read event from the {@link java.nio.channels.Selector}, then the
 * {@link #handle()} is called to read out the data from the socket into a bytebuffer and hand it over to the
 * {@link ReadHandler} to get processed.
 */
public final class NonBlockingSocketReader extends AbstractHandler implements SocketReader {

    @Probe(name = "eventCount")
    private final SwCounter eventCount = newSwCounter();
    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private final MetricsRegistry metricRegistry;

    private ReadHandler readHandler;
    private ByteBuffer inputBuffer;
    private volatile long lastReadTime;

    public NonBlockingSocketReader(
            TcpIpConnection connection,
            NonBlockingIOThread ioThread,
            MetricsRegistry metricsRegistry) {
        super(connection, ioThread, SelectionKey.OP_READ);
        this.ioThread = ioThread;
        this.metricRegistry = metricsRegistry;
        metricRegistry.scanAndRegister(this, "tcp.connection[" + connection.getMetricsId() + "].in");
    }

    @Probe(name = "idleTimeMs", level = DEBUG)
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastReadTime, 0);
    }

    @Override
    public Counter getNormalFramesReadCounter() {
        return normalFramesRead;
    }

    @Override
    public Counter getPriorityFramesReadCounter() {
        return priorityFramesRead;
    }

    @Override
    public long getLastReadTimeMillis() {
        return lastReadTime;
    }

    @Override
    public long getEventCount() {
        return eventCount.get();
    }

    @Override
    public void init() {
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    getSelectionKey();
                } catch (Throwable t) {
                    onFailure(t);
                }
            }
        });
    }

    /**
     * Migrates this handler to a new NonBlockingIOThread.
     * The migration logic is rather simple:
     * <p><ul>
     * <li>Submit a de-registration task to a current NonBlockingIOThread</li>
     * <li>The de-registration task submits a registration task to the new NonBlockingIOThread</li>
     * </ul></p>
     *
     * @param newOwner target NonBlockingIOThread this handler migrates to
     */
    @Override
    public void requestMigration(NonBlockingIOThread newOwner) {
        ioThread.addTaskAndWakeup(new StartMigrationTask(newOwner));
    }

    @Override
    public void handle() throws Exception {
        eventCount.inc();
        // we are going to set the timestamp even if the socketChannel is going to fail reading. In that case
        // the connection is going to be closed anyway.
        lastReadTime = currentTimeMillis();

        if (!connection.isAlive()) {
            logger.finest("We are being asked to read, but connection is not live so we won't");
            return;
        }

        if (readHandler == null) {
            initReadHandler();
            if (readHandler == null) {
                // when using SSL, we can read 0 bytes since data read from socket can be handshake frames.
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

        bytesRead.inc(readBytes);

        inputBuffer.flip();
        readHandler.onRead(inputBuffer);
        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
    }

    private void initReadHandler() throws IOException {
        if (readHandler != null) {
            return;
        }

        ByteBuffer protocolBuffer = ByteBuffer.allocate(3);
        int readBytes = socketChannel.read(protocolBuffer);
        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (readBytes == 0 && connectionManager.isSSLEnabled()) {
            // when using SSL, we can read 0 bytes since data read from socket can be handshake frames.
            return;
        }

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
                readHandler = new ClientReadHandler(connection, ioService);
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
    }

    private void configureBuffers(int size) {
        inputBuffer = IOUtil.newByteBuffer(size, ioService.isSocketBufferDirect());

        try {
            connection.setReceiveBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + connection + " to "
                    + size + " B.", e);
        }
    }

    @Override
    public void close() {
        //todo:
        // ioThread race, shutdown can end up on the old selector
        metricRegistry.deregister(this);
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    socketChannel.closeInbound();
                } catch (IOException e) {
                    logger.finest("Error while closing inbound", e);
                }
            }
        });
    }

    @Override
    public String toString() {
        return connection + ".socketReader";
    }

    private class StartMigrationTask implements Runnable {
        private final NonBlockingIOThread newOwner;

        public StartMigrationTask(NonBlockingIOThread newOwner) {
            this.newOwner = newOwner;
        }

        @Override
        public void run() {
            // if there is no change, we are done
            if (ioThread == newOwner) {
                return;
            }

            try {
                startMigration(newOwner);
            } catch (Throwable t) {
                onFailure(t);
            }
        }
    }
}

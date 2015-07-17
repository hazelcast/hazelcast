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

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.ascii.SocketTextReader;
import com.hazelcast.util.Clock;
import com.hazelcast.util.counters.Counter;
import com.hazelcast.util.counters.SwCounter;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * The reading side of the {@link com.hazelcast.nio.Connection}.
 */
public final class ReadHandler extends AbstractSelectionHandler {

    private final ByteBuffer inputBuffer;

    @Probe(name = "in.bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "in.normalPacketsRead")
    private final SwCounter normalPacketsRead = newSwCounter();
    @Probe(name = "in.priorityPacketsRead")
    private final SwCounter priorityPacketsRead = newSwCounter();
    @Probe(name = "in.exceptionCount")
    private final SwCounter exceptionCount = newSwCounter();
    private final MetricsRegistry metricRegistry;

    private SocketReader socketReader;

    private volatile long lastReadTime;

    //This field will be incremented by a single thread. It can be read by multiple threads.
    @Probe(name = "in.eventCount")
    private final SwCounter eventCount = newSwCounter();

    public ReadHandler(TcpIpConnection connection, IOSelector ioSelector) {
        super(connection, ioSelector, SelectionKey.OP_READ);
        this.ioSelector = ioSelector;
        this.inputBuffer = ByteBuffer.allocate(connectionManager.socketReceiveBufferSize);

        this.metricRegistry = connection.getConnectionManager().getMetricRegistry();
        metricRegistry.scanAndRegister(this, "tcp.connection[" + connection.getMetricsId() + "]");
    }

    @Probe(name = "in.idleTimeMs")
    private long idleTimeMs() {
        return Math.max(System.currentTimeMillis() - lastReadTime, 0);
    }

    @Probe(name = "in.interestedOps")
    private long interestOps() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(name = "in.readyOps")
    private long readyOps() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    public Counter getNormalPacketsRead() {
        return normalPacketsRead;
    }

    public Counter getPriorityPacketsRead() {
        return priorityPacketsRead;
    }

    public void start() {
        ioSelector.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                getSelectionKey();

            }
        });
    }

    @Override
    public long getEventCount() {
        return eventCount.get();
    }

    /**
     * Migrates this handler to a new IOSelector thread.
     * The migration logic is rather simple:
     * <p><ul>
     * <li>Submit a de-registration task to a current IOSelector thread</li>
     * <li>The de-registration task submits a registration task to the new IOSelector thread</li>
     * </ul></p>
     *
     * @param newOwner target IOSelector this handler migrates to
     */
    @Override
    public void requestMigration(IOSelector newOwner) {
        ioSelector.addTaskAndWakeup(new StartMigrationTask(newOwner));
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "eventCount is accessed by a single thread only.")
    public void handle() {
        eventCount.inc();
        lastReadTime = Clock.currentTimeMillis();
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
            } else {
                bytesRead.inc(readBytes);
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

    @Override
    void handleSocketException(Throwable e) {
        exceptionCount.inc();
        super.handleSocketException(e);
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
                    writeHandler.setProtocol(Protocols.CLIENT_BINARY_NEW);
                    socketReader = new SocketClientMessageReader(connection, socketChannel);
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

    long getLastReadTime() {
        return lastReadTime;
    }

    void shutdown() {
        //todo:
        // ioSelector race, shutdown can end up on the old selector
        metricRegistry.deregister(this);
        ioSelector.addTaskAndWakeup(new Runnable() {
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
        return connection + ".readHandler";
    }

    private class StartMigrationTask implements Runnable {
        private final IOSelector newOwner;

        public StartMigrationTask(IOSelector newOwner) {
            this.newOwner = newOwner;
        }

        @Override
        public void run() {
            // if there is no change, we are done
            if (ioSelector == newOwner) {
                return;
            }

            startMigration(newOwner);
        }
    }
}

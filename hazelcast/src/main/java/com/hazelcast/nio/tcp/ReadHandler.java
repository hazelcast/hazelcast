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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.util.StringUtil.bytesToString;

/**
 * The reading side of the {@link com.hazelcast.nio.Connection}.
 */
public final class ReadHandler extends AbstractSelectionHandler {

    private final ByteBuffer inputBuffer;

    private SocketReader socketReader;

    private volatile long lastHandle;

    //This field will be incremented by a single thread. It can be read by multiple threads.
    private volatile long eventCount;

    public ReadHandler(TcpIpConnection connection, IOSelector ioSelector) {
        super(connection, ioSelector, SelectionKey.OP_READ);
        this.ioSelector = ioSelector;
        this.inputBuffer = ByteBuffer.allocate(connectionManager.socketReceiveBufferSize);
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
        return eventCount;
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
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "eventCount is accessed by a single thread only.")
    public void handle() {
        eventCount++;
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

    long getLastHandle() {
        return lastHandle;
    }

    void shutdown() {
        //todo:
        // ioSelector race, shutdown can end up on the old selector
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

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
import com.hazelcast.nio.ascii.TextWriteHandler;
import com.hazelcast.nio.tcp.ClientWriteHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * The writing side of the {@link TcpIpConnection}.
 */
public final class NonBlockingSocketWriter
        extends AbstractNonBlockingSocketWriter<TcpIpConnection> {

    private static final long TIMEOUT = 3;

    private final TcpIpConnectionManager connectionManager;
    private final IOService ioService;

    NonBlockingSocketWriter(TcpIpConnection connection, NonBlockingIOThread ioThread, ILogger logger, IOBalancer balancer) {
        super(connection, connection.getSocketChannelWrapper(), ioThread, logger, balancer);
        this.connectionManager = connection.getConnectionManager();
        this.ioService = connectionManager.getIoService();
    }

    // accessed from ReadHandler and SocketConnector
    @Override
    public void setProtocol(final String protocol) {
        final CountDownLatch latch = new CountDownLatch(1);
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    createWriterHandler(protocol);
                } catch (Throwable t) {
                    onFailure(t);
                } finally {
                    latch.countDown();
                }
            }
        });

        try {
            latch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    @Override
    protected WriteHandler createWriterHandler() throws IOException {
        logger.log(Level.WARNING, "WriteHandler is not set, creating WriteHandler with CLUSTER protocol!");
        return createWriterHandler(CLUSTER);
    }

    private WriteHandler createWriterHandler(String protocol) throws IOException {
        if (CLUSTER.equals(protocol)) {
            configureBuffers(ioService.getSocketSendBufferSize() * KILO_BYTE);
            WriteHandler writeHandler = ioService.createWriteHandler(connection);
            outputBuffer.put(stringToBytes(CLUSTER));
            registerOp(SelectionKey.OP_WRITE);
            return writeHandler;
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            configureBuffers(ioService.getSocketClientReceiveBufferSize() * KILO_BYTE);
            return new ClientWriteHandler();
        } else {
            configureBuffers(ioService.getSocketClientSendBufferSize() * KILO_BYTE);
            return new TextWriteHandler(connection);
        }
    }

    private void configureBuffers(int size) {
        super.configureBuffers(size, ioService.isSocketBufferDirect());

        try {
            connection.setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + connection + " to " + size + " B.", e);
        }
    }
}

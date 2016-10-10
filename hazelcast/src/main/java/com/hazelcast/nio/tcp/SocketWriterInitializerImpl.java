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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.ascii.TextWriteHandler;

import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class SocketWriterInitializerImpl implements SocketWriterInitializer<TcpIpConnection> {

    private final ILogger logger;

    public SocketWriterInitializerImpl(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public void init(TcpIpConnection connection, SocketWriter writer, String protocol) {
        logger.log(Level.WARNING, "SocketWriter is not set, creating WriteHandler with CLUSTER protocol!");

        initHandler(connection, writer, protocol);
        initOutputBuffer(connection, writer, protocol);
    }

    private void initHandler(TcpIpConnection connection, SocketWriter writer, String protocol) {
        WriteHandler handler;
        if (CLUSTER.equals(protocol)) {
            IOService ioService = connection.getConnectionManager().getIoService();
            handler = ioService.createWriteHandler(connection);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            handler = new ClientWriteHandler();
        } else {
            handler = new TextWriteHandler(connection);
        }
        writer.initWriteHandler(handler);
    }

    private void initOutputBuffer(TcpIpConnection connection, SocketWriter writer, String protocol) {
        IOService ioService = connection.getConnectionManager().getIoService();
        int sizeKb = CLUSTER.equals(protocol)
                ? ioService.getSocketSendBufferSize()
                : ioService.getSocketClientReceiveBufferSize();
        int size = KILO_BYTE * sizeKb;

        ByteBuffer outputBuffer = newByteBuffer(size, ioService.isSocketBufferDirect());
        if (CLUSTER.equals(protocol)) {
            outputBuffer.put(stringToBytes(CLUSTER));
        }

        writer.initOutputBuffer(outputBuffer);

        try {
            connection.setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + connection + " to " + size + " B.", e);
        }
    }
}

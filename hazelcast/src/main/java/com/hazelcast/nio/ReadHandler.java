/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.nio.ascii.SocketTextReader;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.logging.Level;

class ReadHandler extends AbstractSelectionHandler implements Runnable {

    final ByteBuffer inBuffer;

    final ByteBuffer protocolBuffer = ByteBuffer.allocate(3);

    SocketReader socketReader = null;

    public ReadHandler(Connection connection) {
        super(connection);
        inBuffer = ByteBuffer.allocate(node.connectionManager.SOCKET_RECEIVE_BUFFER_SIZE);
    }

    public final void handle() {
        if (!connection.live()) {
            logger.log(Level.FINEST, ">>>> We are being to asked to read, but connection is not live so we won't");
            return;
        }
        try {
            if (socketReader == null) {
                int readBytes = socketChannel.read(protocolBuffer);
                if (readBytes == -1) {
                    connection.close();
                    return;
                }
                if (!protocolBuffer.hasRemaining()) {
                    String protocol = new String(protocolBuffer.array());
                    WriteHandler writeHandler = connection.getWriteHandler();
                    if ("HZC".equals(protocol)) {
                        writeHandler.setProtocol("HZC");
                        socketReader = new SocketPacketReader(node, socketChannel, connection);
                    } else {
                        writeHandler.setProtocol("TEXT");
                        inBuffer.put(protocolBuffer.array());
                        socketReader = new SocketTextReader(node, connection);
                        connection.connectionManager.incrementTextConnections();
                    }
                }
            }
            if (socketReader == null) return;
            int readBytes = socketChannel.read(inBuffer);
            if (readBytes == -1) {
                connection.close();
                return;
            }
        } catch (Throwable e) {
            handleSocketException(e);
            return;
        }
        try {
            if (inBuffer.position() == 0) return;
            inBuffer.flip();
            socketReader.read(inBuffer);
            if (inBuffer.hasRemaining()) {
                inBuffer.compact();
            } else {
                inBuffer.clear();
            }
        } catch (Exception t) {
            handleSocketException(t);
        } finally {
            registerOp(inSelector.selector, SelectionKey.OP_READ);
        }
    }

    public final void run() {
        registerOp(inSelector.selector, SelectionKey.OP_READ);
    }
}

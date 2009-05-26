/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ClusterService;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.logging.Level;

class ReadHandler extends AbstractSelectionHandler implements Runnable {

    final ByteBuffer inBuffer = ByteBuffer.allocate(RECEIVE_SOCKET_BUFFER_SIZE);

    Packet packet = null;

    public ReadHandler(final Connection connection) {
        super(connection);
    }

    public final void handle() {

        if (!connection.live())
            return;
        try {
            final int readBytes = socketChannel.read(inBuffer);
            if (readBytes == -1) {
                // End of stream. Closing channel...
                connection.close();
                return;
            }
            if (readBytes <= 0) {
                return;
            }
        } catch (final Exception e) {
            if (packet != null) {
                packet.returnToContainer();
                packet = null;
            }
            handleSocketException(e);
            return;
        }
        try {
            inBuffer.flip();
            while (true) {
                final int remaining = inBuffer.remaining();
                if (remaining <= 0) {
                    inBuffer.clear();
                    return;
                }
                if (packet == null) {
                    if (remaining >= 12) {
                        packet = obtainReadable();
                        if (packet == null) {
                            throw new RuntimeException("Unknown message type  from "
                                    + connection.getEndPoint());
                        }
                    } else {
                        inBuffer.compact();
                        return;
                    }
                }
                final boolean full = packet.read(inBuffer);
                if (full) {
                    packet.flipBuffers();
                    packet.read();
                    packet.setFromConnection(connection);
                    ClusterService.get().enqueueAndReturn(packet);
                    packet = null;
                } else {
                    if (inBuffer.hasRemaining()) {
                        if (DEBUG) {
                            throw new RuntimeException("inbuffer has remaining "
                                    + inBuffer.remaining());
                        }
                    }
                }
            }
        } catch (final Throwable t) {
            logger.log(Level.SEVERE, "Fatal Error at ReadHandler for endPoint: "
                    + connection.getEndPoint(), t);
        } finally {
            registerOp(inSelector.selector, SelectionKey.OP_READ);
        }
    }

    public final void run() {
        registerOp(inSelector.selector, SelectionKey.OP_READ);
    }

    private Packet obtainReadable() {
        final Packet packet = ThreadContext.get().getPacketPool().obtain();
        packet.reset();
        packet.local = false;
        return packet;
    }

}

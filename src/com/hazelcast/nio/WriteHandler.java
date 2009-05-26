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

import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private final Queue writeQueue = new ConcurrentLinkedQueue();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer socketBB = ByteBuffer.allocateDirect(SEND_SOCKET_BUFFER_SIZE);

    private boolean ready = false;

    private Packet lastPacket = null;


    WriteHandler(final Connection connection) {
        super(connection);
    }

    public void enqueuePacket(final Packet packet) {
        writeQueue.offer(packet);
        if (informSelector.get()) {
            informSelector.set(false);
            outSelector.addTask(this);
            if (packet.currentCallCount < 2) {
                outSelector.selector.wakeup();
            }
        }
    }

    public void handle() {
        if (lastPacket == null) {
            lastPacket = (Packet) writeQueue.poll();
            if (lastPacket == null) {
                ready = true;
                return;
            }
        }
        if (!connection.live())
            return;
        try {
            loop:
            while (socketBB.hasRemaining()) {
                if (lastPacket == null) {
                    lastPacket = (Packet) writeQueue.poll();
                }
                if (lastPacket != null) {
                    boolean packetDone = lastPacket.writeToSocketBuffer(socketBB);
                    if (packetDone) {
                        lastPacket.returnToContainer();
                        lastPacket = null;
                    }
                } else break loop;
            }
            socketBB.flip();
            try {
                socketChannel.write(socketBB);
            } catch (final Exception e) {
                if (lastPacket != null) {
                    lastPacket.returnToContainer();
                    lastPacket = null;
                }
                handleSocketException(e);
                return;
            }
            if (socketBB.hasRemaining()) {
                socketBB.compact();
            } else {
                socketBB.clear();
            }

        } catch (final Throwable t) {
            logger.log(Level.SEVERE, "Fatal Error at WriteHandler for endPoint: " + connection.getEndPoint(), t);
        } finally {
            ready = false;
            registerWrite();
        }
    }

    public void run() {
        informSelector.set(true);
        if (ready) {
            handle();
        } else {
            registerWrite();
        }
        ready = false;
    }

    private void registerWrite() {
        registerOp(outSelector.selector, SelectionKey.OP_WRITE);
    }

    @Override
    public void shutdown() {
        Packet packet = (Packet) writeQueue.poll();
        while (packet != null) {
            packet.returnToContainer();
            packet = (Packet) writeQueue.poll();
        }
        writeQueue.clear();
    }
}

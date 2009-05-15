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

import com.hazelcast.impl.Node;
import com.hazelcast.nio.PacketQueue.Packet;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class WriteHandler extends AbstractSelectionHandler implements Runnable {

    private final BlockingQueue writeQueue = new LinkedBlockingQueue();

    private final AtomicBoolean informSelector = new AtomicBoolean(true);

    private final ByteBuffer socketBB = ByteBuffer.allocateDirect(32 * 1024);

    private boolean ready = false;

    private Packet lastPacket = null;


    WriteHandler(final Connection connection) {
        super(connection);
    }

    public void enqueuePacket(final PacketQueue.Packet packet) {
        try {
            writeQueue.put(packet);
        } catch (final InterruptedException e) {
            Node.get().handleInterruptedException(Thread.currentThread(), e);
        }
        if (informSelector.get()) {
            informSelector.set(false);
            outSelector.addTask(this);
            if (packet.currentCallCount < 2) {
                outSelector.selector.wakeup();
            }
        }
    }


    public void handle() {
        if (lastPacket == null && writeQueue.size() == 0) {
            ready = true;
            return;
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
                    boolean packetDone = lastPacket.doWrite(socketBB);
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
        PacketQueue.Packet packet = (Packet) writeQueue.poll();
        while (packet != null) {
            packet.returnToContainer();
            packet = (PacketQueue.Packet) writeQueue.poll();
        }
        writeQueue.clear();
    }
}

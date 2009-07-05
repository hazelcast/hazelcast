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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.impl.ThreadContext;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.logging.Level;

class ReadHandler extends AbstractSelectionHandler implements Runnable {

    final ByteBuffer inBuffer = ByteBuffer.allocate(RECEIVE_SOCKET_BUFFER_SIZE);

    Packet packet = null;

    final PacketReader packetReader;

    public ReadHandler(final Connection connection) {
        super(connection, false);
        if (cipherBuilder != null) {
            if (cipherBuilder.isAsymmetric()) {
                packetReader = new AsymmetricCipherPacketReader();
            } else {
                packetReader = new SymmetricCipherPacketReader();
            }
        } else {
            packetReader = new DefaultPacketReader();
        }
        System.out.println("READHandler " + packetReader);
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
            if (inBuffer.position() == 0) return;
            inBuffer.flip();
            packetReader.readPacket();
            if (inBuffer.hasRemaining()) {
                inBuffer.compact();
            } else {
                inBuffer.clear();
            }

        } catch (final Throwable t) {
            logger.log(Level.SEVERE, "Fatal Error at ReadHandler for endPoint: "
                    + connection.getEndPoint(), t);
        } finally {
            registerOp(inSelector.selector, SelectionKey.OP_READ);
        }
    }

    void enqueueFullPacket(Packet p) {
        p.flipBuffers();
        p.read();
        p.setFromConnection(connection);
        ClusterService.get().enqueueAndReturn(p);
    }

    interface PacketReader {
        void readPacket() throws Exception;
    }

    class DefaultPacketReader implements PacketReader {
        public void readPacket() {
            while (inBuffer.hasRemaining()) {
                if (packet == null) {
                    packet = obtainReadable();
                }
                boolean complete = packet.read(inBuffer);
                if (complete) {
                    enqueueFullPacket(packet);
                    packet = null;
                }
            }
        }
    }

    class AsymmetricCipherPacketReader implements PacketReader {
        Cipher cipher = cipherBuilder.getReaderCipher();
        ByteBuffer cipherBuffer = ByteBuffer.allocate(128);

        public void readPacket() throws Exception{
            while (inBuffer.remaining() >= 128) {
                if (cipherBuffer.position() > 0) throw new RuntimeException();
                int oldLimit = inBuffer.limit();
                inBuffer.limit(inBuffer.position() + 128);
                int cipherReadSize = cipher.doFinal(inBuffer, cipherBuffer);
                inBuffer.limit(oldLimit);

                cipherBuffer.flip();
                while (cipherBuffer.hasRemaining()) {
                    if (packet == null) {
                        packet = obtainReadable();
                    }
                    boolean complete = false;
                    complete = packet.read(cipherBuffer);
                    if (complete) {
                        enqueueFullPacket(packet);
                        packet = null;
                    }
                }
                cipherBuffer.clear();
            }
        }
    }

    class SymmetricCipherPacketReader implements PacketReader {
        int size = -1;
        Cipher cipher = cipherBuilder.getReaderCipher();
        ByteBuffer cipherBuffer = ByteBuffer.allocate(2 * RECEIVE_SOCKET_BUFFER_SIZE);

        public void readPacket()  throws Exception{
            while (inBuffer.hasRemaining()) {
                try {
                    if (size == -1) {
                        if (inBuffer.remaining() < 4) return;
                        size = inBuffer.getInt();
                    }
                    int remaining = inBuffer.remaining();
                    if (remaining < size) {
                        cipher.update(inBuffer, cipherBuffer);
                        size -= remaining;
                    } else if (remaining == size) {
                        cipher.doFinal(inBuffer, cipherBuffer);
                        size = -1;
                    } else {
                        int oldLimit = inBuffer.limit();
                        int newLimit = inBuffer.position() + size;
                        inBuffer.limit(newLimit);
                        cipher.doFinal(inBuffer, cipherBuffer);
                        inBuffer.limit(oldLimit);
                        size = -1;
                    }
                } catch (ShortBufferException e) {
                    e.printStackTrace();
                }
                cipherBuffer.flip();
                while (cipherBuffer.hasRemaining()) {
                    if (packet == null) {
                        packet = obtainReadable();
                    }
                    boolean complete = packet.read(cipherBuffer);
                    if (complete) {
                        enqueueFullPacket(packet);
                        packet = null;
                    }
                }
                cipherBuffer.clear();
            }
        }
    }

    void log(String str) {
//        System.out.println(str);
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

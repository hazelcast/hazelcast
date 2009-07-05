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

import javax.crypto.Cipher;
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

    private final PacketWriter packetWriter;

    WriteHandler(final Connection connection) {
        super(connection, true);
        if (cipherBuilder != null) {
            if (cipherBuilder.isAsymmetric()) {
                packetWriter = new AsymmetricCipherPacketWriter();
            } else {
                packetWriter = new SymmetricCipherPacketWriter();
            }
        } else {
            packetWriter = new DefaultPacketWriter();
        }
        System.out.println("WRITEHandler " + packetWriter);
    }

    long enqueueTime = 0;

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
            while (socketBB.hasRemaining()) {
                if (lastPacket == null) {
                    lastPacket = (Packet) writeQueue.poll();
                }
                if (lastPacket != null) {
                    boolean packetDone = packetWriter.writePacket(lastPacket);
                    if (packetDone) {
                        lastPacket.returnToContainer();
                        lastPacket = null;
                    } else {
                        if (socketBB.hasRemaining()) {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            socketBB.flip();
            try {
                int written = socketChannel.write(socketBB);
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
            t.printStackTrace();
            System.exit(0);
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

    interface PacketWriter {
        boolean writePacket(Packet packet) throws Exception;
    }

    class DefaultPacketWriter implements PacketWriter {
        public boolean writePacket(Packet packet) {
            return packet.writeToSocketBuffer(socketBB);
        }
    }

    class AsymmetricCipherPacketWriter implements PacketWriter {
        ByteBuffer cipherBuffer = ByteBuffer.allocate(2 * SEND_SOCKET_BUFFER_SIZE);
        Cipher cipher = cipherBuilder.getWriterCipher();

        public boolean writePacket(Packet packet) throws Exception {
            return encryptAndWrite(packet);
        }

        public final boolean encryptAndWrite(Packet packet) throws Exception {
            if (cipherBuffer.position() > 0 && socketBB.hasRemaining()) {
                cipherBuffer.flip();
                BufferUtil.copyToDirectBuffer(cipherBuffer, socketBB);
                if (cipherBuffer.hasRemaining()) {
                    cipherBuffer.compact();
                } else {
                    cipherBuffer.clear();
                }
            }
            packet.totalWritten += encryptAndWriteToSocket(packet.bbSizes);
            packet.totalWritten += encryptAndWriteToSocket(packet.bbHeader);
            if (packet.key.size() > 0) {
                int len = packet.key.lsData.size();
                for (int i = 0; i < len && socketBB.hasRemaining(); i++) {
                    ByteBuffer bb = packet.key.lsData.get(i);
                    packet.totalWritten += encryptAndWriteToSocket(bb);
                }
            }

            if (packet.value.size() > 0) {
                int len = packet.value.lsData.size();
                for (int i = 0; i < len && socketBB.hasRemaining(); i++) {
                    ByteBuffer bb = packet.value.lsData.get(i);
                    packet.totalWritten += encryptAndWriteToSocket(bb);
                }
            }
            return packet.totalWritten >= packet.totalSize;
        }

        private final int encryptAndWriteToSocket(ByteBuffer src) throws Exception {
            int remaining = src.remaining();
            if (src.hasRemaining()) {
                doCipherUpdate(src);
                cipherBuffer.flip();
                BufferUtil.copyToDirectBuffer(cipherBuffer, socketBB);
                if (cipherBuffer.hasRemaining()) {
                    cipherBuffer.compact();
                } else {
                    cipherBuffer.clear();
                }
                return remaining - src.remaining();
            }
            return 0;
        }

        private void doCipherUpdate(ByteBuffer src) throws Exception {
            while (src.hasRemaining()) {

                int remaining = src.remaining();
                if (remaining > 100) {
                    int oldLimit = src.limit();
                    src.limit(src.position() + 100);
                    int outputAppendSize = cipher.doFinal(src, cipherBuffer);
                    src.limit(oldLimit);

                } else {

                    int outputAppendSize = cipher.doFinal(src, cipherBuffer);
                }
            }
        }
    }

    class SymmetricCipherPacketWriter implements PacketWriter {
        boolean sizeWritten = false;
        ByteBuffer cipherBuffer = ByteBuffer.allocate(2 * SEND_SOCKET_BUFFER_SIZE);
        Cipher cipher = cipherBuilder.getWriterCipher();
                                    
        public boolean writePacket(Packet packet) throws Exception {
            return encryptAndWrite(packet);
        }

        public final boolean encryptAndWrite(Packet packet) throws Exception {
            if (cipherBuffer.position() > 0 && socketBB.hasRemaining()) {
                cipherBuffer.flip();
                BufferUtil.copyToDirectBuffer(cipherBuffer, socketBB);
                if (cipherBuffer.hasRemaining()) {
                    cipherBuffer.compact();
                } else {
                    cipherBuffer.clear();
                }
            }
            if (!sizeWritten) {
                int cipherSize = cipher.getOutputSize(packet.totalSize);
                socketBB.putInt(cipherSize);
                sizeWritten = true;
            }
            packet.totalWritten += encryptAndWriteToSocket(packet.bbSizes);
            packet.totalWritten += encryptAndWriteToSocket(packet.bbHeader);
            if (packet.key.size() > 0) {
                int len = packet.key.lsData.size();
                for (int i = 0; i < len && socketBB.hasRemaining(); i++) {
                    ByteBuffer bb = packet.key.lsData.get(i);
                    packet.totalWritten += encryptAndWriteToSocket(bb);
                }
            }

            if (packet.value.size() > 0) {
                int len = packet.value.lsData.size();
                for (int i = 0; i < len && socketBB.hasRemaining(); i++) {
                    ByteBuffer bb = packet.value.lsData.get(i);
                    packet.totalWritten += encryptAndWriteToSocket(bb);
                }
            }
            boolean complete = packet.totalWritten >= packet.totalSize;
            if (complete) {
                if (socketBB.remaining() >= cipher.getOutputSize(0)) {
                    sizeWritten = false;
                    socketBB.put(cipher.doFinal());
                } else {
                    return false;
                }
            }
            return complete;
        }

        private final int encryptAndWriteToSocket(ByteBuffer src) throws Exception {
            int remaining = src.remaining();
            if (src.hasRemaining()) {
                cipher.update(src, cipherBuffer);
                cipherBuffer.flip();
                BufferUtil.copyToDirectBuffer(cipherBuffer, socketBB);
                if (cipherBuffer.hasRemaining()) {
                    cipherBuffer.compact();
                } else {
                    cipherBuffer.clear();
                }
                return remaining - src.remaining();
            }
            return 0;
        }
    }

    void log(String str) {
//        System.out.println(str);
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

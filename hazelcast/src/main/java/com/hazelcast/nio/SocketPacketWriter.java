/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.logging.ILogger;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.copyToDirectBuffer;

class SocketPacketWriter implements SocketWriter<Packet> {

    private final PacketWriter packetWriter;
    final TcpIpConnection connection;
    final IOService ioService;
    final ILogger logger;

    SocketPacketWriter(TcpIpConnection connection) {
        this.connection = connection;
        this.ioService = connection.getConnectionManager().ioService;
        this.logger = ioService.getLogger(SocketPacketWriter.class.getName());
        boolean symmetricEncryptionEnabled = CipherHelper.isSymmetricEncryptionEnabled(ioService);
        if (symmetricEncryptionEnabled) {
            packetWriter = new SymmetricCipherPacketWriter();
            logger.info("Writer started with SymmetricEncryption");
        } else {
            packetWriter = new DefaultPacketWriter();
        }
    }

    public boolean write(Packet socketWritable, ByteBuffer socketBuffer) throws Exception {
        return packetWriter.writePacket(socketWritable, socketBuffer);
    }

    private interface PacketWriter {
        boolean writePacket(Packet packet, ByteBuffer socketBB) throws Exception;
    }

    private class DefaultPacketWriter implements PacketWriter {
        public boolean writePacket(Packet packet, ByteBuffer socketBB) {
            return packet.writeTo(socketBB);
        }
    }

    private class SymmetricCipherPacketWriter implements PacketWriter {
        boolean sizeWritten = false;
        ByteBuffer packetBuffer = ByteBuffer.allocate(ioService.getSocketSendBufferSize() * IOService.KILO_BYTE);
        final ByteBuffer cipherBuffer = ByteBuffer.allocate(ioService.getSocketSendBufferSize() * IOService.KILO_BYTE);
        final Cipher cipher;

        SymmetricCipherPacketWriter() {
            Cipher c = null;
            try {
                c = CipherHelper.createSymmetricWriterCipher(connection.getConnectionManager().ioService);
            } catch (Exception e) {
                logger.severe("Symmetric Cipher for WriteHandler cannot be initialized.", e);
                CipherHelper.handleCipherException(e, connection);
            }
            cipher = c;
        }

        public boolean writePacket(Packet packet, ByteBuffer socketBB) throws Exception {
            if (!sizeWritten) {
                if (packetBuffer.capacity() < packet.size()) {
                    packetBuffer = ByteBuffer.allocate(packet.size());
                }
                if (!packet.writeTo(packetBuffer)) {
                    throw new RuntimeException("Packet didn't fit into the buffer");
                }
                packetBuffer.flip();
            }
            if (cipherBuffer.position() > 0 && socketBB.hasRemaining()) {
                cipherBuffer.flip();
                copyToDirectBuffer(cipherBuffer, socketBB);
                if (cipherBuffer.hasRemaining()) {
                    cipherBuffer.compact();
                } else {
                    cipherBuffer.clear();
                }
            }
            if (!sizeWritten) {
                if (socketBB.remaining() > 4) {
                    int cipherSize = cipher.getOutputSize(packet.size());
                    socketBB.putInt(cipherSize);
                    sizeWritten = true;
                } else {
                    return false;
                }
            }
            encryptAndWriteToSocket(packetBuffer, socketBB);
            boolean complete = !packetBuffer.hasRemaining();
            if (complete) {
                if (socketBB.remaining() >= cipher.getOutputSize(0)) {
                    sizeWritten = false;
                    socketBB.put(cipher.doFinal());
                    packetBuffer.clear();
                } else {
                    return false;
                }
            }
            return complete;
        }

        private int encryptAndWriteToSocket(ByteBuffer src, ByteBuffer socketBB) throws Exception {
            int remaining = src.remaining();
            if (src.hasRemaining() && cipherBuffer.hasRemaining()) {
                int outputSize = cipher.getOutputSize(src.remaining());
                if (outputSize <= cipherBuffer.remaining()) {
                    cipher.update(src, cipherBuffer);
                } else {
                    int min = Math.min(src.remaining(), cipherBuffer.remaining());
                    int len = min / 2;
                    if (len > 0) {
                        int limitOld = src.limit();
                        src.limit(src.position() + len);
                        cipher.update(src, cipherBuffer);
                        src.limit(limitOld);
                    } else {
                        return 0;
                    }
                }
                cipherBuffer.flip();
                copyToDirectBuffer(cipherBuffer, socketBB);
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
}

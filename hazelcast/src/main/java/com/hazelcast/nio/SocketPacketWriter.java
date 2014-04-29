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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ExceptionUtil;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;

class SocketPacketWriter implements SocketWriter<Packet> {

    private static final int CONST_BUFFER_NO = 4;
    final TcpIpConnection connection;
    final IOService ioService;
    final ILogger logger;

    private final PacketWriter packetWriter;

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

    private static class DefaultPacketWriter implements PacketWriter {
        public boolean writePacket(Packet packet, ByteBuffer socketBB) {
            return packet.writeTo(socketBB);
        }
    }

    private class SymmetricCipherPacketWriter implements PacketWriter {
        final Cipher cipher;
        ByteBuffer packetBuffer = ByteBuffer.allocate(ioService.getSocketSendBufferSize() * IOService.KILO_BYTE);
        boolean packetWritten;

        SymmetricCipherPacketWriter() {
            cipher = init();
        }

        private Cipher init() {
            Cipher c;
            try {
                c = CipherHelper.createSymmetricWriterCipher(ioService.getSymmetricEncryptionConfig());
            } catch (Exception e) {
                logger.severe("Symmetric Cipher for WriteHandler cannot be initialized.", e);
                CipherHelper.handleCipherException(e, connection);
                throw ExceptionUtil.rethrow(e);
            }
            return c;
        }

        public boolean writePacket(Packet packet, ByteBuffer socketBuffer) throws Exception {
            if (!packetWritten) {
                if (socketBuffer.remaining() < CONST_BUFFER_NO) {
                    return false;
                }
                int size = cipher.getOutputSize(packet.size());
                socketBuffer.putInt(size);

                if (packetBuffer.capacity() < packet.size()) {
                    packetBuffer = ByteBuffer.allocate(packet.size());
                }
                if (!packet.writeTo(packetBuffer)) {
                    throw new HazelcastException("Packet didn't fit into the buffer!");
                }
                packetBuffer.flip();
                packetWritten = true;
            }

            if (socketBuffer.hasRemaining()) {
                int outputSize = cipher.getOutputSize(packetBuffer.remaining());
                if (outputSize <= socketBuffer.remaining()) {
                    cipher.update(packetBuffer, socketBuffer);
                } else {
                    int min = Math.min(packetBuffer.remaining(), socketBuffer.remaining());
                    int len = min / 2;
                    if (len > 0) {
                        int limitOld = packetBuffer.limit();
                        packetBuffer.limit(packetBuffer.position() + len);
                        cipher.update(packetBuffer, socketBuffer);
                        packetBuffer.limit(limitOld);
                    }
                }

                if (!packetBuffer.hasRemaining()) {
                    if (socketBuffer.remaining() >= cipher.getOutputSize(0)) {
                        socketBuffer.put(cipher.doFinal());
                        packetWritten = false;
                        packetBuffer.clear();
                        return true;
                    }
                }
            }
            return false;
        }
    }
}

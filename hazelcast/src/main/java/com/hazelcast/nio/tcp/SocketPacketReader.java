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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.CipherHelper;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.ExceptionUtil;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;

class SocketPacketReader implements SocketReader {

    private static final int CONST_BUFFER_NO = 4;

    Packet packet;

    final PacketReader packetReader;
    final TcpIpConnection connection;
    final IOService ioService;
    final ILogger logger;

    public SocketPacketReader(TcpIpConnection connection) {
        this.connection = connection;
        this.ioService = connection.getConnectionManager().ioService;
        this.logger = ioService.getLogger(getClass().getName());
        boolean symmetricEncryptionEnabled = CipherHelper.isSymmetricEncryptionEnabled(ioService);
        if (symmetricEncryptionEnabled) {
            packetReader = new SymmetricCipherPacketReader();
            logger.info("Reader started with SymmetricEncryption");
        } else {
            packetReader = new DefaultPacketReader();
        }
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        packetReader.readPacket(inBuffer);
    }

    private void enqueueFullPacket(final Packet p) {
        p.setConn(connection);
        ioService.handleMemberPacket(p);
    }

    private interface PacketReader {
        void readPacket(ByteBuffer inBuffer) throws Exception;
    }

    private class DefaultPacketReader implements PacketReader {
        public void readPacket(ByteBuffer inBuffer) {
            while (inBuffer.hasRemaining()) {
                if (packet == null) {
                    packet = obtainReadable();
                }
                boolean complete = packet.readFrom(inBuffer);
                if (complete) {
                    enqueueFullPacket(packet);
                    packet = null;
                } else {
                    break;
                }
            }
        }
    }

    private final class SymmetricCipherPacketReader implements PacketReader {
        int size = -1;
        final Cipher cipher;
        ByteBuffer cipherBuffer = ByteBuffer.allocate(ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE);

        private SymmetricCipherPacketReader() {
            cipher = init();
        }

        Cipher init() {
            Cipher c;
            try {
                c = CipherHelper.createSymmetricReaderCipher(ioService.getSymmetricEncryptionConfig());
            } catch (Exception e) {
                logger.severe("Symmetric Cipher for ReadHandler cannot be initialized.", e);
                CipherHelper.handleCipherException(e, connection);
                throw ExceptionUtil.rethrow(e);
            }
            return c;
        }

        public void readPacket(ByteBuffer inBuffer) throws Exception {
            while (inBuffer.hasRemaining()) {
                try {
                    if (size == -1) {
                        if (inBuffer.remaining() < CONST_BUFFER_NO) {
                            return;
                        }
                        size = inBuffer.getInt();
                        if (cipherBuffer.capacity() < size) {
                            cipherBuffer = ByteBuffer.allocate(size);
                        }
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
                    logger.warning(e);
                }
                cipherBuffer.flip();
                while (cipherBuffer.hasRemaining()) {
                    if (packet == null) {
                        packet = obtainReadable();
                    }
                    boolean complete = packet.readFrom(cipherBuffer);
                    if (complete) {
                        enqueueFullPacket(packet);
                        packet = null;
                    }
                }
                cipherBuffer.clear();
            }
        }
    }

    public Packet obtainReadable() {
        return new Packet(ioService.getSerializationContext());
    }
}
